import os
from pipeline.nypd_arrests_ELT.utils.file_checker import file_exists
from pipeline.nypd_arrests_ELT.utils.duckdb_connection import attach_pg, connect_duckdb, configure_s3_duckdb

def check_table_exists(db_conn, db_schema, table_name):
    table_exists = db_conn.sql(
        f"""
        select exists (
            select 1
            from pg.information_schema.tables
            where table_schema = '{db_schema}'
              and table_name = '{table_name}'
        );
        """
    ).fetchone()[0]

    return table_exists


def check_final_status(year_end: int):
    db_conn = connect_duckdb()
    attach_pg(db_conn)
    result = db_conn.execute(
        f"""
        select year, month, extract_status, dbt_status
        from pg.schedules.extraction_schedule
        where year = {year_end} and month = 12
        """
    ).fetchone()

    if result is None:
        return None

    # Return as a dict for easier use
    columns = ["year", "month", "extract_status", "dbt_status"]
    return dict(zip(columns, result))


def load_pg_dim(s3, db_conn, dimensions):

    db_schema = "raw"

    for dim in dimensions:

        key = f"raw/dimensions/raw_dim_{dim}.parquet"
        table_name = f"{db_schema}_dim_{dim}"

        # Check if the Parquet file exists in S3
        s3_exists = file_exists(s3, key)

        # Check if the PostgreSQL table exists
        table_exists = check_table_exists(db_conn, db_schema, table_name)

        # Check if file exists and table also exists, skip if both True
        if s3_exists and table_exists:
            print(f"Skipping {table_name}: S3 file and PostgreSQL table already exist.")
            continue

        # S3 exists + PostgreSQL missing
        if s3_exists and not table_exists:
            print(
                f"Loading {table_name} from S3 into PostgreSQL..."
            )
            parquet_path = (
                f"s3://{os.getenv('bucket')}/"
                f"{key}"
            )

            db_conn.execute(
                f"""
                CREATE TABLE pg.{db_schema}.{table_name} AS
                SELECT *
                FROM read_parquet('{parquet_path}');
                """
            )

            print(
                f"Loaded {table_name} "
                "from S3."
            )

            continue

        # S3 missing + PostgreSQL exists
        if not s3_exists and table_exists:
            print(
                f"Skipping {table_name}: "
                "PostgreSQL table already exists, "
                "but S3 file is missing."
            )
            continue

        # S3 missing + PostgreSQL missing
        if not s3_exists and not table_exists:
            print(
                f"Skipping {table_name}: "
                "S3 file and PostgreSQL table "
                "do not exist. Extraction required."
            )
            continue



# check if batch exists in raw_processed.processed_raw_arrests table using sched_id
def check_batch_exists(api_params):
    table_name = "processed_raw_arrests"
    db_schema = "raw_processed"
    # table_name = "raw_nypd_arrests"
    # db_schema = "raw"
    check_batch_conn = connect_duckdb()
    try:
        attach_pg(check_batch_conn)
        batch_exists = check_batch_conn.execute(
            f"""
            select exists (
            select 1
            from pg.{db_schema}.{table_name}
            where sched_id = {api_params['sched_id']}
            );
            """
        ).fetchone()[0]
        return batch_exists
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        check_batch_conn.close()


# Execute only if extraction is done for the specified year and month
def load_combined_raw_data(api_params, combined_path):

    db_conn = connect_duckdb()

    try:
        configure_s3_duckdb(db_conn)
        attach_pg(db_conn)

        table_name = "raw_nypd_arrests"
        db_schema = "raw"

        # create empty table frame if not exists
        db_conn.execute(
            f"""
            create table if not exists pg.{db_schema}.{table_name} as
            select
            *,
            cast(null as integer) as sched_id,
            cast(null as timestamp) as loaded_at
            from read_parquet('{combined_path}')
            limit 0;
            """
        )

        # check if batch exists
        batch_exists = check_batch_exists(api_params)
        if batch_exists:
            print(f"Batch: {api_params['sched_id']} exists.")
            return "BATCH_EXISTS"

        # insert raw data to pg.raw.raw_nypd_arrests
        db_conn.execute(
            f"""
            insert into pg.{db_schema}.{table_name}
            select
                *,
                {api_params['sched_id']} as sched_id,
                current_timestamp as loaded_at
            from read_parquet('{combined_path}');
            """
        )
        print(f"Batch: {api_params['sched_id']} loaded.")
        return 'BATCH_LOADED'

    finally:
        db_conn.close()
