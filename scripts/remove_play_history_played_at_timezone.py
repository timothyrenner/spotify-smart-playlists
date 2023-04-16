import duckdb
import typer


def main(database_file: str = "spotify.db"):
    # Strips the timezone from the played_at field in play_history.
    database = duckdb.connect(database_file)
    database.execute(
        """
    CREATE OR REPLACE TABLE play_history_tz_corrected AS
    SELECT 
        track_id,
        (played_at AT TIME ZONE 'UTC')::TIMESTAMP AS played_at
    FROM play_history
    """
    )

    database.execute(
        """
    CREATE OR REPLACE TABLE play_history AS
    SELECT * FROM play_history_tz_corrected
    """
    )


if __name__ == "__main__":
    typer.run(main)
