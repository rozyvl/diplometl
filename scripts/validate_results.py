import psycopg2


DB_CONFIG = {
    "host": "dwh-postgres",
    "port": 5432,
    "database": "taxi_dwh",
    "user": "dwh_user",
    "password": "dwh_password",
}


def get_single_value(cursor, query: str):
    cursor.execute(query)
    return cursor.fetchone()[0]


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cursor:
            staging_rows = get_single_value(
                cursor,
                "SELECT COUNT(*) FROM staging.stg_yellow_taxi_trips;"
            )

            fact_rows = get_single_value(
                cursor,
                "SELECT COUNT(*) FROM dwh.fact_trips;"
            )

            mart_rows = get_single_value(
                cursor,
                "SELECT COUNT(*) FROM mart.v_daily_taxi_report;"
            )

            quality_rows = get_single_value(
                cursor,
                "SELECT COUNT(*) FROM dwh.data_quality_results;"
            )

            dates_out_of_range = get_single_value(
                cursor,
                """
                SELECT COUNT(*)
                FROM mart.v_daily_taxi_report
                WHERE report_date < DATE '2024-01-01'
                   OR report_date >= DATE '2024-02-01';
                """
            )

            print("Validation results:")
            print(f"staging rows: {staging_rows}")
            print(f"fact rows: {fact_rows}")
            print(f"mart rows: {mart_rows}")
            print(f"data quality checks: {quality_rows}")
            print(f"dates out of range in mart: {dates_out_of_range}")

            if staging_rows == 0:
                raise ValueError("Validation failed: staging table is empty")

            if fact_rows == 0:
                raise ValueError("Validation failed: fact_trips table is empty")

            if mart_rows == 0:
                raise ValueError("Validation failed: mart view is empty")

            if quality_rows == 0:
                raise ValueError("Validation failed: data quality results are empty")

            if dates_out_of_range > 0:
                raise ValueError("Validation failed: mart contains dates out of January 2024")

    finally:
        conn.close()

    print("Validation completed successfully")


if __name__ == "__main__":
    main()