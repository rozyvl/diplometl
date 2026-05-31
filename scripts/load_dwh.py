from pathlib import Path
from io import StringIO
import time

import pandas as pd
import psycopg2


LOCATION_FILE = Path("/opt/airflow/data/raw/taxi_zone_lookup.csv")

DB_CONFIG = {
    "host": "dwh-postgres",
    "port": 5432,
    "database": "taxi_dwh",
    "user": "dwh_user",
    "password": "dwh_password",
}


def copy_location_lookup(cursor) -> None:
    cursor.execute("""
        CREATE TEMP TABLE tmp_location_lookup (
            location_id INTEGER,
            borough VARCHAR(100),
            zone VARCHAR(255),
            service_zone VARCHAR(100)
        );
    """)

    if not LOCATION_FILE.exists():
        print("Location lookup file not found. dim_location will be filled with Unknown values.")
        return

    df = pd.read_csv(LOCATION_FILE)

    df = df.rename(columns={
        "LocationID": "location_id",
        "Borough": "borough",
        "Zone": "zone",
        "service_zone": "service_zone"
    })

    df = df[["location_id", "borough", "zone", "service_zone"]]

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cursor.copy_expert("""
        COPY tmp_location_lookup (location_id, borough, zone, service_zone)
        FROM STDIN
        WITH (FORMAT CSV, NULL '\\N')
    """, buffer)


def insert_data_quality_results(cursor) -> None:
    cursor.execute("""
        INSERT INTO dwh.data_quality_results (
            check_name,
            check_status,
            checked_rows,
            failed_rows,
            comment
        )
        SELECT
            'Общее количество строк в staging',
            'INFO',
            COUNT(*),
            0,
            'Количество строк, загруженных в staging-слой'
        FROM staging.stg_yellow_taxi_trips;
    """)

    checks = [
        (
            "Проверка дат поездки",
            """
            tpep_pickup_datetime IS NULL
            OR tpep_dropoff_datetime IS NULL
            OR tpep_pickup_datetime >= tpep_dropoff_datetime
            """,
            "Дата начала поездки должна быть меньше даты окончания"
        ),
        (
            "Проверка диапазона дат",
            """
            tpep_pickup_datetime < '2024-01-01'
            OR tpep_pickup_datetime >= '2024-02-01'
            OR tpep_dropoff_datetime < '2024-01-01'
            OR tpep_dropoff_datetime >= '2024-02-01'
            """,
            "Дата поездки должна находиться в пределах января 2024 года"
        ),
        (
            "Проверка расстояния поездки",
            """
            trip_distance IS NULL
            OR trip_distance <= 0
            """,
            "Расстояние поездки должно быть больше нуля"
        ),
        (
            "Проверка стоимости поездки",
            """
            total_amount IS NULL
            OR total_amount < 0
            """,
            "Итоговая стоимость поездки не должна быть отрицательной"
        ),
        (
            "Проверка количества пассажиров",
            """
            passenger_count IS NULL
            OR passenger_count <= 0
            """,
            "Количество пассажиров должно быть больше нуля"
        ),
        (
            "Проверка обязательных идентификаторов",
            """
            vendor_id IS NULL
            OR payment_type_id IS NULL
            OR pickup_location_id IS NULL
            OR dropoff_location_id IS NULL
            """,
            "Обязательные идентификаторы не должны быть пустыми"
        ),
    ]

    for check_name, condition, comment in checks:
        cursor.execute(f"""
            INSERT INTO dwh.data_quality_results (
                check_name,
                check_status,
                checked_rows,
                failed_rows,
                comment
            )
            SELECT
                %s,
                CASE WHEN SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) = 0
                    THEN 'PASS'
                    ELSE 'FAIL'
                END,
                COUNT(*),
                SUM(CASE WHEN {condition} THEN 1 ELSE 0 END),
                %s
            FROM staging.stg_yellow_taxi_trips;
        """, (check_name, comment))


def load_dimensions(cursor) -> None:
    cursor.execute("""
        WITH valid_trips AS (
            SELECT *
            FROM staging.stg_yellow_taxi_trips
            WHERE tpep_pickup_datetime IS NOT NULL
            AND tpep_dropoff_datetime IS NOT NULL
            AND tpep_pickup_datetime < tpep_dropoff_datetime
            AND tpep_pickup_datetime >= '2024-01-01'
            AND tpep_pickup_datetime < '2024-02-01'
            AND tpep_dropoff_datetime >= '2024-01-01'
            AND tpep_dropoff_datetime < '2024-02-01'
            AND trip_distance > 0
            AND total_amount >= 0
            AND passenger_count > 0
            AND vendor_id IS NOT NULL
            AND payment_type_id IS NOT NULL
            AND pickup_location_id IS NOT NULL
            AND dropoff_location_id IS NOT NULL
        ),
        dates AS (
            SELECT DISTINCT tpep_pickup_datetime::date AS full_date
            FROM valid_trips
            UNION
            SELECT DISTINCT tpep_dropoff_datetime::date AS full_date
            FROM valid_trips
        )
        INSERT INTO dwh.dim_date (
            date_id,
            full_date,
            year,
            quarter,
            month,
            day,
            day_of_week,
            day_name,
            is_weekend
        )
        SELECT
            TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_id,
            full_date,
            EXTRACT(YEAR FROM full_date)::INTEGER,
            EXTRACT(QUARTER FROM full_date)::INTEGER,
            EXTRACT(MONTH FROM full_date)::INTEGER,
            EXTRACT(DAY FROM full_date)::INTEGER,
            EXTRACT(DOW FROM full_date)::INTEGER,
            TO_CHAR(full_date, 'Day'),
            CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END
        FROM dates
        ON CONFLICT (date_id) DO NOTHING;
    """)

    cursor.execute("""
        WITH valid_vendors AS (
            SELECT DISTINCT vendor_id
            FROM staging.stg_yellow_taxi_trips
            WHERE vendor_id IS NOT NULL
        )
        INSERT INTO dwh.dim_vendor (vendor_id, vendor_name)
        SELECT
            vendor_id,
            CASE vendor_id
                WHEN 1 THEN 'Creative Mobile Technologies'
                WHEN 2 THEN 'VeriFone Inc.'
                ELSE 'Unknown vendor ' || vendor_id::TEXT
            END AS vendor_name
        FROM valid_vendors
        ON CONFLICT (vendor_id) DO NOTHING;
    """)

    cursor.execute("""
        WITH valid_payment_types AS (
            SELECT DISTINCT payment_type_id
            FROM staging.stg_yellow_taxi_trips
            WHERE payment_type_id IS NOT NULL
        )
        INSERT INTO dwh.dim_payment_type (payment_type_id, payment_type_name)
        SELECT
            payment_type_id,
            CASE payment_type_id
                WHEN 1 THEN 'Credit card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided trip'
                ELSE 'Other'
            END AS payment_type_name
        FROM valid_payment_types
        ON CONFLICT (payment_type_id) DO NOTHING;
    """)

    cursor.execute("""
        WITH locations AS (
            SELECT DISTINCT pickup_location_id AS location_id
            FROM staging.stg_yellow_taxi_trips
            WHERE pickup_location_id IS NOT NULL

            UNION

            SELECT DISTINCT dropoff_location_id AS location_id
            FROM staging.stg_yellow_taxi_trips
            WHERE dropoff_location_id IS NOT NULL
        )
        INSERT INTO dwh.dim_location (
            location_id,
            borough,
            zone,
            service_zone
        )
        SELECT
            l.location_id,
            COALESCE(t.borough, 'Unknown') AS borough,
            COALESCE(t.zone, 'Unknown') AS zone,
            COALESCE(t.service_zone, 'Unknown') AS service_zone
        FROM locations l
        LEFT JOIN tmp_location_lookup t
            ON l.location_id = t.location_id
        ON CONFLICT (location_id) DO NOTHING;
    """)


def load_facts(cursor) -> None:
    cursor.execute("""
        WITH valid_trips AS (
            SELECT
                *,
                md5(
                    CONCAT_WS(
                        '|',
                        vendor_id,
                        tpep_pickup_datetime,
                        tpep_dropoff_datetime,
                        passenger_count,
                        trip_distance,
                        pickup_location_id,
                        dropoff_location_id,
                        payment_type_id,
                        fare_amount,
                        total_amount
                    )
                ) AS source_row_hash
            FROM staging.stg_yellow_taxi_trips
            WHERE tpep_pickup_datetime IS NOT NULL
              AND tpep_dropoff_datetime IS NOT NULL
              AND tpep_pickup_datetime < tpep_dropoff_datetime
              AND tpep_pickup_datetime >= '2024-01-01'
              AND tpep_pickup_datetime < '2024-02-01'
              AND tpep_dropoff_datetime >= '2024-01-01'
              AND tpep_dropoff_datetime < '2024-02-01'
              AND trip_distance > 0
              AND total_amount >= 0
              AND passenger_count > 0
              AND vendor_id IS NOT NULL
              AND payment_type_id IS NOT NULL
              AND pickup_location_id IS NOT NULL
              AND dropoff_location_id IS NOT NULL
        )
        INSERT INTO dwh.fact_trips (
            vendor_id,
            pickup_date_id,
            dropoff_date_id,
            payment_type_id,
            pickup_location_id,
            dropoff_location_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            fare_amount,
            tip_amount,
            tolls_amount,
            total_amount,
            trip_duration_minutes,
            source_row_hash
        )
        SELECT
            vendor_id,
            TO_CHAR(tpep_pickup_datetime::date, 'YYYYMMDD')::INTEGER,
            TO_CHAR(tpep_dropoff_datetime::date, 'YYYYMMDD')::INTEGER,
            payment_type_id,
            pickup_location_id,
            dropoff_location_id,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            fare_amount,
            tip_amount,
            tolls_amount,
            total_amount,
            ROUND(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0, 2),
            source_row_hash
        FROM valid_trips;
    """)


def main() -> None:
    start_time = time.time()

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                TRUNCATE TABLE
                    dwh.fact_trips,
                    dwh.dim_date,
                    dwh.dim_vendor,
                    dwh.dim_payment_type,
                    dwh.dim_location,
                    dwh.data_quality_results
                RESTART IDENTITY CASCADE;
            """)

            copy_location_lookup(cursor)
            insert_data_quality_results(cursor)
            load_dimensions(cursor)
            load_facts(cursor)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    elapsed_time = round(time.time() - start_time, 2)

    print("DWH load completed successfully")
    print(f"Execution time, seconds: {elapsed_time}")


if __name__ == "__main__":
    main()