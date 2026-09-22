import psycopg2
import os

"""
To establish connection to postgres using psycopg2, this can be used when psycopg2 connection
is desired instead of using duckdb's postgres extension.
"""

def conn_default():
    conn_def = psycopg2.connect(
        dbname = os.getenv('pg_def_dbname'),
        user = os.getenv('pg_def_user'),
        password = os.getenv('pg_def_password'),
        host = os.getenv('pg_def_host'),
        port = os.getenv('pg_def_port')
    )
    return conn_def


def conn_target_db():
    conn_target = psycopg2.connect(
        dbname = os.getenv('pg_nypd_dbname'),
        user = os.getenv('pg_nypd_user'),
        password = os.getenv('pg_nypd_password'),
        host = os.getenv('pg_nypd_host'),
        port = os.getenv('pg_nypd_port')
    )
    return conn_target
