"""
To update the extraction schedule table after each ELT flow run.
"""

from pipeline.nypd_arrests_ELT.utils.duckdb_connection import attach_pg, connect_duckdb

def update_extraction_schedule(result):

    db_conn = connect_duckdb()

    try:
        attach_pg(db_conn)

        db_conn.execute(
            """
            update pg.schedules.extraction_schedule
            set
                extract_status = ?,
                pages_extracted = ?,
                records_extracted = ?,
                error_message = NULL,
                updated_at = current_timestamp
            where sched_id = ?;
            """,
            [
                result["extract_status"],
                result["pages_extracted"],
                result["records_extracted"],
                result["sched_id"]
            ]
        )

    finally:
        db_conn.close()


"""
Executed when the API request is an error
"""
def update_extraction_error(result):

    db_conn = connect_duckdb()

    try:
        attach_pg(db_conn)

        db_conn.execute(
            """
            update pg.schedules.extraction_schedule
            set
                extract_status = 'ERROR',
                extract_attempt_count = extract_attempt_count + 1,
                error_message = ?,
                updated_at = current_timestamp
            where sched_id = ?;
            """,
            [
                result["error_message"],
                result["sched_id"]
            ]
        )

    finally:
        db_conn.close()


"""
Executed when the API request returns no more records
"""
def update_extraction_complete(result):

    db_conn = connect_duckdb()

    try:
        attach_pg(db_conn)

        db_conn.execute(
            """
            update pg.schedules.extraction_schedule
            set
                extract_status = 'COMPLETE',
                pages_extracted = ?,
                records_extracted = ?,
                error_message = NULL,
                last_extracted_at = current_timestamp,
                updated_at = current_timestamp
            where sched_id = ?;
            """,
            [
                result["pages_extracted"],
                result["records_extracted"],
                result["sched_id"]
            ]
        )

    finally:
        db_conn.close()



def update_dbt_complete(dbt_result, result):

    if not dbt_result.success:
        raise RuntimeError(
            "DBT transformation failed. dbt_status update cancelled."
        )

    db_conn = connect_duckdb()

    try:
        attach_pg(db_conn)

        db_conn.execute(
            """
            update pg.schedules.extraction_schedule
            set
                dbt_status = 'COMPLETE',
                updated_at = current_timestamp
            where sched_id = ?;
            """,
            [
                result["sched_id"]
            ]
        )

    finally:
        db_conn.close()
