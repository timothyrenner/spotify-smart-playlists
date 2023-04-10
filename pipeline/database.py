from duckdb import DuckDBPyConnection
from prefect import task, get_run_logger
import polars as pl


@task(name="Check if table exists.")
def table_exists(database: DuckDBPyConnection, table: str) -> bool:
    logger = get_run_logger()
    logger.info(f"Determining if {table} exists.")
    tables = {x[0] for x in database.sql("SHOW TABLES").fetchall()}
    return table in tables


@task(name="Save to database.")
def save_to_database(
    database: DuckDBPyConnection,
    table: str,
    data_frame: pl.DataFrame,
    create_or_replace: bool,
):
    logger = get_run_logger()
    logger.info(
        f"Saving {data_frame.shape[0]} rows to the database in {table}."
    )
    if create_or_replace:
        database.execute(
            f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM data_frame"
        )
    else:
        columns = ",".join(data_frame.columns)
        database.execute(
            f"INSERT INTO {table} ({columns}) SELECT * FROM data_frame"
        )
