import pandas as pd
import requests
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from requests.auth import HTTPBasicAuth
from pipeline.nypd_arrests_ELT.utils.api_creds import api_creds_gen
from pipeline.nypd_arrests_ELT.utils.api_headers_payload import api_headers, payload_dimensions
from pipeline.nypd_arrests_ELT.utils.duckdb_connection import attach_pg, connect_duckdb, configure_s3_duckdb
from pipeline.nypd_arrests_ELT.utils.file_checker import file_exists
from pipeline.nypd_arrests_ELT.utils.file_export import export_file
from pipeline.nypd_arrests_ELT.utils.gen_dim_date import generate_dim_date
from pipeline.nypd_arrests_ELT.utils.load_to_pg import load_pg_dim
from pipeline.nypd_arrests_ELT.utils.s3_boto3_client import get_s3_client
from pipeline.nypd_arrests_ELT.utils.run_dbt_models import run_dbt_stg_dims, run_dbt_marts_dims, dbt_paths


# This script is only required to run once for this project

# Generate dimension data table names
@task(name="dimension-table-names")
def dimensions_data(*dimensions_name):
    if not dimensions_name:
        dimensions = [
            'ofns_desc', 'law_cat_cd', 'arrest_boro', 'arrest_precinct',
            'jurisdiction_code', 'age_group', 'perp_sex', 'perp_race', 'date'
        ]
        return dimensions

    elif len(dimensions_name) == 1 and isinstance(dimensions_name[0], (list, tuple)):
        dimensions = list(dimensions_name[0])
        return dimensions

    else:
        dimensions = list(dimensions_name)
        return dimensions

# Extract: Send API request
@task(name="send-api-request", retries=2, cache_policy=NO_CACHE)
def api_request(api_secrets, dimensions):
    headers = api_headers()
    raw_dimensions = {}
    for item in dimensions:
        raw_dimensions[item] = []
        try:
            payload_dim = payload_dimensions(item)
            response = requests.post(
                api_secrets["url"],
                json=payload_dim,
                headers=headers,
                auth=HTTPBasicAuth(api_secrets["api_key_id"], api_secrets["api_key_secret"])
            )
            response.raise_for_status()
            data = response.json()
            raw_dimensions[item].extend(data)
            print(f"Raw dimension data for {item} extracted.")

        except Exception as e:
            print(f"Error extracting {item}: {e}")
            raise
    return raw_dimensions

# Load 1: Load to object storage s3(RustFS)
@task(name='export-data-parquet', cache_policy=NO_CACHE)
def export_dimensions(s3, raw_dimensions):
    for dim_name, values in raw_dimensions.items():
        if not values:
            continue

        filename = f"raw_dim_{dim_name}.parquet"
        key = f"raw/dimensions/{filename}"
        df = pd.DataFrame(values)
        export_file(s3, df, key)

# Load 2: Load to data warehouse after all raw dimensions is extracted.
@task(name="load-raw-dimension-to-postgres", cache_policy=NO_CACHE)
def load_to_postgres(s3, dimensions):
    db_conn = connect_duckdb()
    try:
        configure_s3_duckdb(db_conn)
        attach_pg(db_conn)
        load_pg_dim(s3, db_conn, dimensions)
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        db_conn.close()

# Transform: Run DBT models
@task(name='run-dbt-dimensions', cache_policy=NO_CACHE)
def run_dbt():
    dbt_proj_dir, dbt_profiles_dir = dbt_paths()
    try:
        # staging dimensions
        run_dbt_stg_dims(dbt_proj_dir, dbt_profiles_dir)

        # marts dimensions
        run_dbt_marts_dims(dbt_proj_dir, dbt_profiles_dir)
    except Exception as e:
        print(f"Error occured: {e}")
        raise

@flow(name="flow-extract-load-transform-dimensions")
def flow_elt_dim():

    # Initialize utility functions
    api_secrets = api_creds_gen()
    s3 = get_s3_client()

    # Extract dimensions, save in s3, load to postgres
    dimensions = dimensions_data()

    for dim_name in dimensions:
        filename = f"raw_dim_{dim_name}.parquet"
        key = f"raw/dimensions/{filename}"

        if file_exists(s3, key):
            print(f"Skipping API request for raw_dim_{dim_name}.parquet, file already exists.")
            continue
        if dim_name == 'date': # date dimension will be created and not extracted from the api
            df = generate_dim_date('2024-01-01','2026-01-01')
            export_file(s3, df, key)
        else:
            raw_dimensions = api_request(api_secrets, [dim_name])
            export_dimensions(s3, raw_dimensions)

    # load to postgres
    load_to_postgres(s3, dimensions)

    # Run dbt transformations
    run_dbt()

if __name__ == '__main__':
    flow_elt_dim()
