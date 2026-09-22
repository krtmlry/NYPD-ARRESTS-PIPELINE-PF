from pathlib import Path
from dbt.cli.main import dbtRunner

def dbt_paths():
    project_root = Path(__file__).resolve().parents[3]

    dbt_proj_dir = project_root / "dbt_nypd_arrests_pg"
    dbt_profiles_dir = project_root / "dbt_nypd_arrests_pg"

    return dbt_proj_dir, dbt_profiles_dir

# WARNING: run includes downstream models
def run_dbt_commands(selector: str, dbt_proj_dir:str , dbt_profiles_dir:str):
    dbt = dbtRunner()
    commands = [
        "run",
        "--select", selector,
        "--project-dir", str(dbt_proj_dir),
        "--profiles-dir", str(dbt_profiles_dir)
    ]
    try:
        result = dbt.invoke(commands)

        if not result.success:
            print(result.exception)
            return result

        print(f"DBT {selector} and its upstream models completed successfully.")
        return result

    except Exception as e:
        print(f"Error running DBT models: {e}")
        raise


def run_dbt_stg_dims(dbt_proj_dir, dbt_profiles_dir):
    dbt = dbtRunner()

    commands = [
        "run",
        "--select", "tag:dimension",
        "--project-dir", str(dbt_proj_dir),
        "--profiles-dir", str(dbt_profiles_dir)
    ]

    try:
        result = dbt.invoke(commands)

        if not result.success:
            print("DBT staging dimension models failed.")
            print(result.exception)
            return result

        print("DBT staging dimension models completed successfully.")
        return result

    except Exception as e:
        print(f"Error running DBT staging models: {e}")
        raise

def run_dbt_marts_dims(dbt_proj_dir, dbt_profiles_dir):
    dbt = dbtRunner()
    commands = [
        "run",
        "--select", "tag:marts",
        "--project-dir", str(dbt_proj_dir),
        "--profiles-dir", str(dbt_profiles_dir)
    ]
    try:
        result = dbt.invoke(commands)

        if not result.success:
            print("DBT marts dimension models failed.")
            print(result.exception)
            return result

        print("DBT marts dimension models completed successfully.")
        return result

    except Exception as e:
        print(f"Error running DBT marts models: {e}")
        raise

def run_dbt_stg_arrests(dbt_proj_dir, dbt_profiles_dir):
    dbt = dbtRunner()
    commands = [
        "run",
        "--select", "tag:stg_arrests",
        "stg_nypd_arrests",
        "--project-dir", str(dbt_proj_dir),
        "--profiles-dir", str(dbt_profiles_dir)
    ]
    try:
        result = dbt.invoke(commands)

        if not result.success:
            raise RuntimeError("dbt staging arrests models failed.")

        return result

    except Exception as e:
        print(f"Error running DBT staging arrests: {e}")
        raise


def run_dbt_int_arrests(dbt_proj_dir, dbt_profiles_dir):
    dbt = dbtRunner()
    commands = [
        "run",
        "--select", "tag:int_fact",
        "int_fact_nypd_arrests",
        "--project-dir", str(dbt_proj_dir),
        "--profiles-dir", str(dbt_profiles_dir)
    ]
    try:
        result = dbt.invoke(commands)

        if not result.success:
            raise RuntimeError("dbt int arrests models failed.")

        return result

    except Exception as e:
        print(f"Error running DBT int arrests: {e}")
        raise

def run_dbt_fact_arrests(dbt_proj_dir, dbt_profiles_dir):
    dbt = dbtRunner()
    commands = [
        "run",
        "--select", "tag:int_fact",
        "int_fact_nypd_arrests",
        "--project-dir", str(dbt_proj_dir),
        "--profiles-dir", str(dbt_profiles_dir)
    ]
    try:
        result = dbt.invoke(commands)

        if not result.success:
            raise RuntimeError("dbt int arrests models failed.")

        return result

    except Exception as e:
        print(f"Error running DBT int arrests: {e}")
        raise
