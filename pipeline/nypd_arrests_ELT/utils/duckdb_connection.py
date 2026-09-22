import duckdb as db
import os


def connect_duckdb():
    db_conn = db.connect(":memory:")
    return db_conn

def install_load_ext_httpfs(db_conn):
    # Install and load s3 extensions
    db_conn.execute("INSTALL httpfs")
    db_conn.execute("LOAD httpfs")


def install_load_ext_postgres(db_conn):
    # Install and load Postgres extensions
    db_conn.execute("INSTALL postgres")
    db_conn.execute("LOAD postgres")

def configure_s3_duckdb(db_conn):
    install_load_ext_httpfs(db_conn)
    # RustFS / S3 configuration
    try:
        db_conn.execute(
            f"""
            SET s3_endpoint='{os.getenv("duckdb_s3_endpoint")}';
            SET s3_access_key_id='{os.getenv("access_key")}';
            SET s3_secret_access_key='{os.getenv("access_secret_key")}';
            SET s3_region='us-east-1';
            SET s3_url_style='path';
            SET s3_use_ssl=false;
            """
        )
    except Exception as e:
        print(f"Error: {e}")
        raise

def attach_pg(db_conn):
    """Attach the PostgreSQL database to DuckDB."""
    install_load_ext_postgres(db_conn)
    try:
        db_conn.execute(
            f"""
            ATTACH '
            host={os.getenv("pg_nypd_host")}
            port={os.getenv("pg_nypd_port")}
            dbname={os.getenv("pg_nypd_dbname")}
            user={os.getenv("pg_nypd_user")}
            password={os.getenv("pg_nypd_password")}
            '
            AS pg (TYPE POSTGRES);
            """
        )
    except Exception as e:
        print(f"Error: {e}")
        raise
