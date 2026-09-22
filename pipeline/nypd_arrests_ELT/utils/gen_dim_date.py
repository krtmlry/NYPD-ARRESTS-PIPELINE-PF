import duckdb as db

"""
To generate date dimension data, date range can be changed accordingly.
date range format: 'YYYY-MM-DD'
"""

def generate_dim_date(start_range: str, end_range:str):
    df_dim_date = db.sql(
        f"""
        with q1 as(
            select cast(range as date) as date
            from range(date '{start_range}', date '{end_range}', interval 1 day)
        )
        select
            cast(strftime(date, '%Y%m%d') as integer) as date_id,
            date,
            date_part('year', date) as year,
            date_part('month', date) as month,
            date_part('day', date) as day,
            date_part('quarter', date) as quarter,
            strftime(date, '%B') as month_name,
            strftime(date, '%b') as month_name_abb,
            date_part('dow', date) as dow,
            upper(substr(strftime(date, '%A'), 1, 1)) || lower(substr(strftime(date, '%A'), 2)) as dow_name,
            upper(substr(strftime(date, '%a'), 1, 1)) || lower(substr(strftime(date, '%a'), 2)) as dow_name_abb
        from q1
        """
    ).df()
    return df_dim_date
