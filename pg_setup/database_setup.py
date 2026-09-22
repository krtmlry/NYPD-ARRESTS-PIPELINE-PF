"""
To create target database, schemas and schedules.extraction_schedule table.
"""

import psycopg2
from pipeline.nypd_arrests_ELT.utils.pg_connection import conn_default, conn_target_db
from pipeline.nypd_arrests_ELT.utils.duckdb_connection import connect_duckdb, attach_pg

def connect_to_default():
    conn_def = conn_default()
    conn_def.autocommit = True
    cur = conn_def.cursor()
    try:
        cur.execute("CREATE DATABASE nypd_arrests_db;")
        print("Database 'nypd_arrests_db' created.")
    except psycopg2.errors.DuplicateDatabase:
        print("Database already exists, skipping.")
    cur.close()
    conn_def.close()

def connect_to_target():
    conn_target = conn_target_db()
    cur = conn_target.cursor()

    schemas = ["staging", "marts", "schedules", "intermediate", "raw", "fact", "raw_processed"]
    for schema in schemas:
        cur.execute(
            """
            SELECT
            schema_name
            FROM information_schema.schemata
            WHERE schema_name = %s;
            """,
            (schema,)
        )
        exists = cur.fetchone()
        if exists:
            print(f"Schema '{schema}' already exists.")
        else:
            cur.execute(f"CREATE SCHEMA {schema};")
            print(f"Schema '{schema}' created.")

    # Show all schemas at the end
    cur.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;")
    all_schemas = cur.fetchall()
    print("Schemas in database:", [s[0] for s in all_schemas])

    conn_target.commit()
    cur.close()
    conn_target.close()

def create_extraction_schedule(start_year: int, end_year: int):
    conn = connect_duckdb()
    try:
        attach_pg(conn)

        # Check if table already exists, stop execution to avoid conflicts.
        table_exists = conn.execute(
            """
            select 1
            from information_schema.tables
            where table_schema = 'schedules'
              and table_name = 'extraction_schedule';
            """
        ).fetchone()

        if table_exists:
            print("Table 'pg.schedules.extraction_schedule' already exists. Stopping execution.")
            return

        # Create schedules schema
        conn.execute(
            """
            create schema if not exists pg.schedules;
            """
        )

        # Create extraction schedule
        conn.execute(
            """
            create table if not exists pg.schedules.extraction_schedule (
                sched_id integer primary key,
                year integer not null,
                month integer not null,
                extract_status varchar not null default 'PENDING',
                dbt_status varchar not null default 'PENDING',
                pages_extracted integer not null default 0,
                records_extracted integer not null default 0,
                extract_attempt_count integer not null default 0,
                error_message varchar,
                last_extracted_at timestamp,
                updated_at timestamp default current_timestamp,
                unique (year, month)
            );
            """
        )

        # Generate year/month schedule
        conn.execute(
            f"""
            insert into pg.schedules.extraction_schedule (
                sched_id,
                year,
                month
            )
            select
                (y.year * 100 + m.month) as sched_id,
                year,
                month
            from range({start_year}, {end_year + 1}) as y(year)
            cross join range(1, 13) as m(month)
            on conflict(year, month) do nothing;
            """
        )

    finally:
        conn.close()


def main():
    connect_to_default()
    connect_to_target()
    create_extraction_schedule(2024, 2025)

if __name__ == '__main__':
    main()
