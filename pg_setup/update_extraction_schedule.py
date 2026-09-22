"""
Execute this script to add additional years and months to the existing extraction schedule table.
Removing records not needed for the schedule, if necessary it must be done on the database's query tool.
"""


from pipeline.nypd_arrests_ELT.utils.duckdb_connection import connect_duckdb, attach_pg


def add_extraction_years(start_year:int, end_year:int):
    conn = connect_duckdb()
    try:
        attach_pg(conn)
        conn.execute(
            f"""
            insert into pg.schedules.extraction_schedule (
                sched_id,
                year,
                month
            )
            select
                (y.year * 100 + m.month) as sched_id,
                y.year,
                m.month
            from range(
                {start_year},
                {end_year + 1}
            ) as y(year)
            cross join range(
                1,
                13
            ) as m(month)
            where not exists (
                select 1
                from pg.schedules.extraction_schedule s
                where s.year = y.year
                  and s.month = m.month
            );
            """
        )

    finally:
        conn.close()


if __name__ == "__main__":

    start_year = 2023 # sample range, can be adjusted
    end_year = 2023 # sample range, can be adjusted
    add_extraction_years(start_year, end_year)
