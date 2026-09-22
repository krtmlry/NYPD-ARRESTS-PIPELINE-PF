import os

def api_headers():
    headers = {"X-App-Token": os.getenv('app_token')}
    return headers

def payload_dimensions(dim:str):
    payload_dim = {
        "query":f"select distinct {dim}",
        "includeSynthetic": False
    }
    return payload_dim

def payload_arrests(year_start:int, month:int, page_number:int, page_size:int):
    payload_arr = {
        "query": f"""
        select
            arrest_key,
            arrest_date,
            ofns_desc,
            law_cat_cd,
            arrest_boro,
            arrest_precinct,
            jurisdiction_code,
            age_group,
            perp_sex,
            perp_race,
            latitude,
            longitude
        where date_extract_y(arrest_date)={year_start}
        and date_extract_m(arrest_date)={month}
        """,
        "includeSynthetic": False,
        "page": {"pageNumber": page_number, "pageSize": page_size},
    }
    return payload_arr
