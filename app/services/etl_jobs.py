import time

from onetl.connection import Clickhouse, Postgres
from onetl.db import DBReader, DBWriter
from onetl.strategy import IncrementalStrategy
from pyspark.sql import DataFrame, SparkSession

from app.core.config import load_env
from app.core.spark import get_spark
from app.services.db_connections import get_clickhouse_connection, get_postgres_connection
from app.services.transforms import transform_clickstream

TARGET_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    "event_date",
    "event_hour",
    "day_of_week",
    "category_level_1",
    "category_level_2",
    "category_level_3",
]

JARS_PACKAGES = (
    "org.postgresql:postgresql:42.7.3,"
    "com.clickhouse:clickhouse-jdbc:0.6.0"
)

def build_spark(app_name: str) -> SparkSession:
    load_env()
    return get_spark(
        app_name=app_name,
        jars_packages=JARS_PACKAGES,
    )

def select_target_columns(df: DataFrame) -> DataFrame:
    return df.select(*TARGET_COLUMNS)

def write_to_clickhouse_onetl(clickhouse: Clickhouse, df: DataFrame) -> int:
    result_df = select_target_columns(df)
    write_count = result_df.count()

    writer = DBWriter(
        connection=clickhouse,
        table="analytics.dm_events_clean",
        options=Clickhouse.WriteOptions(
            if_exists="append",
        ),
    )

    writer.run(result_df)
    return write_count

def truncate_clickhouse_table(clickhouse: Clickhouse) -> None:
    clickhouse.execute("truncate analytics.dm_events_clean")

def build_result(
    job_type: str,
    status: str,
    duration_seconds: float,
    **extra: int,
) -> dict:
    result = {
        "job_type": job_type,
        "status": status,
        "duration_seconds": duration_seconds,
    }
    result.update(extra)
    return result

def run_full_snapshot_onetl() -> dict:
    start_time = time.time()
    spark = build_spark(app_name="full_snapshot_onetl")

    try:
        print("\nStart full snapshot via onETL\n")

        postgres = get_postgres_connection(spark)
        clickhouse = get_clickhouse_connection(spark)

        reader = DBReader(
            connection=postgres,
            source="raw.events",
        )

        source_df = reader.run()
        transformed_df, before_count, after_count = transform_clickstream(source_df)

        print(f"Rows before filtering: {before_count}")
        print(f"Rows after filtering: {after_count}")

        print("Clear Clickhouse target table")
        truncate_clickhouse_table(clickhouse)

        write_count = write_to_clickhouse_onetl(clickhouse, transformed_df)

        print(f"Rows written to Clickhouse: {write_count}")

        duration = round(time.time() - start_time, 2)
        print(f"Full snapshot finished in {duration} seconds\n")

        return build_result(
            job_type="full_snapshot",
            status="success",
            duration_seconds=duration,
            rows_before_filtering=before_count,
            rows_after_filtering=after_count,
            rows_written=write_count,
        )

    finally:
        spark.stop()

def run_incremental_onetl() -> dict:
    start_time = time.time()
    spark = build_spark(app_name="incremental_onetl")

    try:
        print("\nStart incremental load via onETL\n")

        postgres = get_postgres_connection(spark)
        clickhouse = get_clickhouse_connection(spark)

        reader = DBReader(
            connection=postgres,
            source="raw.events",
            columns=[
                "event_time",
                "event_type",
                "product_id",
                "category_id",
                "category_code",
                "brand",
                "price",
                "user_id",
                "user_session",
            ],
            hwm=DBReader.AutoDetectHWM(
                name="clickstream_event_time_hwm",
                expression="event_time",
            ),
            options=Postgres.ReadOptions(
                fetchsize=5000,
            ),
        )

        with IncrementalStrategy():
            source_df = reader.run()
            source_count = source_df.count()
            print(f"Rows read from source: {source_count}")

            if source_count == 0:
                duration = round(time.time() - start_time, 2)
                print("No new rows found")
                print(f"Incremental load finished in {duration} seconds\n")

                return build_result(
                    job_type="incremental",
                    status="success",
                    duration_seconds=duration,
                    rows_read=0,
                    rows_before_filtering=0,
                    rows_after_filtering=0,
                    rows_written=0,
                )

            transformed_df, before_count, after_count = transform_clickstream(source_df)

            print(f"Rows before filtering: {before_count}")
            print(f"Rows after filtering: {after_count}")

            write_count = write_to_clickhouse_onetl(clickhouse, transformed_df)

            print(f"Rows written to Clickhouse: {write_count}")

        duration = round(time.time() - start_time, 2)
        print(f"Incremental load finished in {duration} seconds\n")

        return build_result(
            job_type="incremental",
            status="success",
            duration_seconds=duration,
            rows_read=source_count,
            rows_before_filtering=before_count,
            rows_after_filtering=after_count,
            rows_written=write_count,
        )

    finally:
        spark.stop()