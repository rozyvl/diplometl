from pathlib import Path
from io import StringIO
import time

import pandas as pd
import psycopg2


RAW_FILE = Path("/opt/airflow/data/raw/yellow_tripdata_2024-01.parquet")

DB_CONFIG = {
    "host": "dwh-postgres",
    "port": 5432,
    "database": "taxi_dwh",
    "user": "dwh_user",
    "password": "dwh_password",
}

MAX_ROWS = 200_000

COLUMN_RENAME = {
    "VendorID": "vendor_id",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type_id",
    "Airport_fee": "airport_fee",
    "airport_fee": "airport_fee",
}

STAGING_COLUMNS = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type_id",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "source_file",
]


def prepare_dataframe() -> pd.DataFrame:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"File not found: {RAW_FILE}")

    df = pd.read_parquet(RAW_FILE)
    raw_rows_count = len(df)

    df = df.rename(columns=COLUMN_RENAME)

    for column in STAGING_COLUMNS:
        if column not in df.columns and column != "source_file":
            df[column] = None

    df = df[[column for column in STAGING_COLUMNS if column != "source_file"]]
    df["source_file"] = RAW_FILE.name

    if MAX_ROWS is not None:
        df = df.head(MAX_ROWS)

    integer_columns = [
        "vendor_id",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type_id",
    ]

    timestamp_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]

    for column in integer_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    for column in timestamp_columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")

    print(f"Raw rows in file: {raw_rows_count}")
    print(f"Rows prepared for staging: {len(df)}")
    print(f"Columns prepared: {list(df.columns)}")

    return df


def load_to_postgres(df: pd.DataFrame) -> None:
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE staging.stg_yellow_taxi_trips;")

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, na_rep="\\N")
            buffer.seek(0)

            columns_sql = ", ".join(STAGING_COLUMNS)

            copy_sql = f"""
                COPY staging.stg_yellow_taxi_trips ({columns_sql})
                FROM STDIN
                WITH (FORMAT CSV, NULL '\\N')
            """

            cursor.copy_expert(copy_sql, buffer)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    start_time = time.time()

    df = prepare_dataframe()
    load_to_postgres(df)

    elapsed_time = round(time.time() - start_time, 2)

    print("Staging load completed successfully")
    print(f"Loaded rows: {len(df)}")
    print(f"Execution time, seconds: {elapsed_time}")


if __name__ == "__main__":
    main()