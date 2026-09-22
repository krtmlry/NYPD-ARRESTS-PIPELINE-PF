import os
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from prefect import task, flow
from pipeline.nypd_arrests_ELT.utils.duckdb_connection import attach_pg, connect_duckdb, configure_s3_duckdb
from pipeline.nypd_arrests_ELT.utils.api_creds import api_creds_gen
from pipeline.nypd_arrests_ELT.utils.api_headers_payload import api_headers, payload_arrests
from pipeline.nypd_arrests_ELT.utils.file_export import export_file
from pipeline.nypd_arrests_ELT.utils.file_checker import file_exists
from pipeline.nypd_arrests_ELT.utils.s3_boto3_client import get_s3_client
from pipeline.nypd_arrests_ELT.utils.load_to_pg import load_combined_raw_data, check_final_status
from pipeline.nypd_arrests_ELT.utils.run_dbt_models import run_dbt_commands, dbt_paths
from pipeline.nypd_arrests_ELT.utils.pg_update_schedules import update_extraction_schedule, update_extraction_error, update_extraction_complete, update_dbt_complete
from pipeline.nypd_arrests_ELT.utils.deployment_control import get_deployment_details, pause_deployment_schedule
from prefect.cache_policies import NO_CACHE


@task(name="get-next-extraction-batch", cache_policy=NO_CACHE)
def get_next_extraction_batch(year_start: int, year_end: int):
    db_conn = connect_duckdb()
    try:
        attach_pg(db_conn)
        result = db_conn.execute(
            f"""
            select *
            from pg.schedules.extraction_schedule
            where year between {year_start} and {year_end}
            and extract_status in ('PENDING','IN_PROGRESS','ERROR')
            order by year, month
            limit 1
            """
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = [c[0] for c in result.description]
        return dict(zip(columns, row))
    finally:
        db_conn.close()


@task(name="get-next-dbt-batch", cache_policy=NO_CACHE)
def get_next_dbt_batch(year_start: int, year_end: int):
    db_conn = connect_duckdb()
    try:
        attach_pg(db_conn)
        result = db_conn.execute(
            f"""
            select *
            from pg.schedules.extraction_schedule
            where year between {year_start} and {year_end}
            and extract_status = 'COMPLETE'
            and dbt_status = 'PENDING'
            order by year, month
            limit 1
            """
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = [c[0] for c in result.description]
        return dict(zip(columns, row))
    finally:
        db_conn.close()


@task(name='extract-arrest-records',cache_policy=NO_CACHE)
def extract_arrest_page(api_params, api_secrets):

    page_number = api_params['pages_extracted'] + 1
    year = api_params['year']
    month = api_params['month']
    page_size = 15000

    headers = api_headers()

    payload_arr = payload_arrests(
        year,
        month,
        page_number,
        page_size
    )

    try:

        response = requests.post(
            api_secrets['url'],
            json=payload_arr,
            headers=headers,
            auth=HTTPBasicAuth(
                api_secrets["api_key_id"],
                api_secrets["api_key_secret"]
            ),
            timeout=180
        )

        response.raise_for_status()

        records = response.json()

    except requests.RequestException as e:

        return {
            **api_params,
            "extract_status": "ERROR",
            "error_message": str(e)
        }

    # Arrest records to process and to return
    df = pd.json_normalize(records)

    # Determine if this is the final page
    if len(records) < page_size:
        extract_status = "COMPLETE"
    else:
        extract_status = "IN_PROGRESS"

    return {
        **api_params,
        "df": df,
        "page_number": page_number,
        "extract_status": extract_status
    }


@task(name='load-page-file-to-s3', cache_policy=NO_CACHE)
def load_page_to_s3(s3, result):

    # execute if extract_status is PENDING
    df = result["df"]

    sched_id = result['sched_id']
    page_number = result['page_number']
    year = result['year']
    month = result['month']

    file_name = (f"{sched_id}_RAW_PAGE_{page_number}.parquet")

    key_uncombined = (
        f"raw/arrests/uncombined/"
        f"{year}/{month:02d}/{file_name}"
    )

    # check if file exists before loading, skip if true
    if file_exists(s3, key_uncombined):
        print(f"File: '{file_name}' already exists. Skipping export.")
    else:
        export_file(s3, df, key_uncombined)
        print(f"File: '{file_name}' exported.")

    load_s3_result = {
        **result,
        "extract_status": result["extract_status"],
        "pages_extracted": page_number,
        "records_extracted": (
            result['records_extracted'] + len(df)
        )
    }

    return load_s3_result


@task(name='combine-monthly-pages',cache_policy=NO_CACHE)
def combine_pages(s3, api_params):
        # Execute if extract_status is COMPLETE
        db_conn = connect_duckdb()
        try:
            configure_s3_duckdb(db_conn)

            bucket = os.getenv('bucket')
            year = api_params['year']
            month = api_params['month']
            merged_filename = f"{year}{month:02d}_RAW_MERGED.parquet"

            uncombined_path = (
                f"s3://{bucket}/"
                f"raw/arrests/uncombined/"
                f"{year}/{month:02d}/*_RAW_PAGE_*.parquet"
            )

            combined_path = (
                f"s3://{bucket}/"
                f"raw/arrests/combined/"
                f"{year}/{month:02d}/{merged_filename}"
            )

            if file_exists(s3, combined_path):
                print("File exists. Skipping merge.")
                return combined_path

            # combine pages
            db_conn.execute(
                f"""
                copy (
                    select
                    *
                    from read_parquet('{uncombined_path}')
                )
                to '{combined_path}'
                (
                    format parquet,
                    compression zstd
                );
                """
            )
            print(f"Pages merged for {year}-{month:02d}")
            return combined_path
        finally:
            db_conn.close()


@task(name='load-merged-data-to-raw.raw_nypd_arrests', cache_policy=NO_CACHE)
def load_to_postgres(s3, api_params, combined_path):

        # Check first if merged file exists in S3.
        if not file_exists(s3, combined_path):
            print(f"Merged file '{combined_path}' does not exist. Skipping load.")
            return "FILE_NOT_FOUND"

        # utility
        load_result = load_combined_raw_data( api_params, combined_path)
        return load_result


# Run DBT transform if extract_status is complete, load_result = 'BATCH_LOADED'
@task(name='run-dbt-transformations',cache_policy=NO_CACHE)
def run_dbt_transforms(load_result):

        # DBT RUN IF BATCH IS LOADED
        if load_result != 'BATCH_LOADED':
            raise RuntimeError(f"DBT Run error: {load_result}")

        # RUN DBT MODELS
        dbt_proj_dir, dbt_profiles_dir = dbt_paths()

        result = run_dbt_commands("stg_nypd_arrests+", dbt_proj_dir, dbt_profiles_dir)

        if not result.success:
            raise RuntimeError("DBT transformation failed.")


        return result


@flow(name='flow-elt-arrests')
def flow_elt_arrests(year_start: int=2024, year_end: int=2025):

        # UTILITY FUNCTIONS
        s3 = get_s3_client()
        api_secrets = api_creds_gen()

        # CHECK IF 2025-12 IS COMPLETE, PAUSE DEPLOYMENT SCHEDULE
        final_status = check_final_status(year_end)
        if (
            final_status
            and final_status["extract_status"] == "COMPLETE"
            and final_status["dbt_status"] == "COMPLETE"
        ):
            print(f"Pipeline complete for {year_start}-{year_end}. Pausing deployment.")
            deployment_details = get_deployment_details()
            pause_deployment_schedule(deployment_details)
            return

        # EXTRACT DBT PARAMS
        dbt_params = get_next_dbt_batch(year_start, year_end)

        if dbt_params:
            print(f"Pending DBT found for {dbt_params['year']}-{dbt_params['month']:02d}.")
            combined_path = combine_pages(s3, dbt_params)
            load_result = load_to_postgres(s3, dbt_params, combined_path)

            if load_result != 'BATCH_LOADED':
                raise RuntimeError(
                    f"DBT precondition failed for "
                    f"{dbt_params['year']}-{dbt_params['month']:02d}: "
                    f"{load_result}"
                )

            dbt_result = run_dbt_transforms(load_result)
            update_dbt_complete(dbt_result, dbt_params)

            return


        # EXTRACT API PARAMETERS
        api_params = get_next_extraction_batch(year_start, year_end)

        if api_params:
            print(
                f"Processing extraction for "
                f"{api_params['year']}-{api_params['month']:02d}"
            )

            # EXTRACT PAGE
            extract_result = extract_arrest_page(api_params, api_secrets)

            # ERROR HANDLER, UPDATE TABLE, STOP EXECUTION
            if extract_result['extract_status'] == 'ERROR':

                update_extraction_error(extract_result)

                raise RuntimeError(
                    f"API extraction failed for "
                    f"{api_params['year']}-{api_params['month']:02d}: "
                    f"{extract_result['error_message']}"
                )

            # LOAD EXTRACTED PAGE RESULT TO S3
            extract_result = load_page_to_s3(s3, extract_result)

            # MORE PAGES REMAIN
            if extract_result['extract_status'] == 'IN_PROGRESS':
                update_extraction_schedule(extract_result)
                return

            # IF FINAL PAGE IS EXTRACTED
            if extract_result['extract_status'] == 'COMPLETE':

                # MERGE ALL PAGES
                combined_path = combine_pages(s3, api_params)

                # LOAD MERGED DATA TO raw.raw_nypd_arrests
                load_result = load_to_postgres(s3, api_params, combined_path)

                print(load_result)

                # DBT ONLY RUNS IF THE RAW BATCH WAS SUCCESSFULLY LOADED
                if load_result != 'BATCH_LOADED':
                    raise RuntimeError(
                        f"Raw load failed for "
                        f"{api_params['year']}-{api_params['month']:02d}: "
                        f"{load_result}"
                    )

                # UPDATE SCHEDULE TABLE
                update_extraction_complete(extract_result)

                # RUN DBT IMMEDIATELY AFTER SUCCESSFUL RAW LOAD
                dbt_result = run_dbt_transforms(load_result)

                # UPDATE dbt_status = COMPLETE
                update_dbt_complete(dbt_result, api_params)

                return

        print("No pending extraction or DBT batch. Exiting.")


if __name__ == '__main__':
    flow_elt_arrests()
