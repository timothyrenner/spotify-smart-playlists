import duckdb
import typer


def main(database_file: str = "spotify.db"):
    database = duckdb.connect(database_file)
    database.execute('DROP TABLE IF EXISTS "all-high-energy"')
    database.execute("DROP TABLE IF EXISTS all_synthwave")
    database.execute('DROP TABLE IF EXISTS "angry-instrumental"')
    database.execute('DROP TABLE IF EXISTS "calm-instrumentals"')
    database.execute('DROP TABLE IF EXISTS "hip-hop"')
    database.execute("DROP TABLE IF EXISTS play_history_tz_corrected")
    database.execute("DROP TABLE IF EXISTS synthwave_flow")
    database.execute("DROP TABLE IF EXISTS synthwave_speed")
    database.execute('DROP TABLE IF EXISTS "wu-tang"')


if __name__ == "__main__":
    typer.run(main)
