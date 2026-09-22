from prefect import flow
from datetime import timedelta

"""
Before execution, a prefect workpool must be created and activated.
"""

if __name__ == "__main__":
    flow.from_source(
        source="/app",
        entrypoint="pipeline/nypd_arrests_ELT/flows/ELT_arrests.py:flow_elt_arrests"
    ).deploy(
        name="deploy-nypd-arrests-pipeline",
        work_pool_name="nypd-pipeline-workpool",
        interval=timedelta(seconds=70),
        concurrency_limit=1
        #parameters={"year_start":2024, "year_end":2025} # use parameters for flexibility.
    )
