import time

from onetl.connection import Clickhouse, Postgres
from onetl.db import DBReader, DBWriter
from onetl.strategy import IncrementalStrategy

from app.core.config import load_env
from app.core.spark import get_spark
from app.services.db_connections import get_postgres_connection, get_clickhouse_connection
from app.services.transforms import transform_clickstream

def write_to_clickhouse_onetl(clickhouse, df):
    result_df = df.select(
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
    )

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

def truncate_clickhouse_table(clickhouse):
    clickhouse.execute("truncate analytics.dm_events_clean")

def run_full_snapshot_onetl():
    start_time = time.time()

    load_env()

    spark=get_spark(
        app_name="full_snapshot_onetl",
        jars_packages="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0",
    )

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

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        print(f"Full snapshot finished in {duration} seconds\n")

    finally:
        spark.stop()

def run_incremental_onetl():
    start_time = time.time()

    load_env()

    spark = get_spark(
        app_name="incremental_onetl",
        jars_packages="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0",
    )

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
                print("No new rows found")
                end_time = time.time()
                duration = round(end_time - start_time, 2)
                print(f"Incremental load finished in {duration} seconds\n")
                return

            transformed_df, before_count, after_count = transform_clickstream(source_df)

            print(f"Rows before filtering: {before_count}")
            print(f"Rows after filtering: {after_count}")

            write_count = write_to_clickhouse_onetl(clickhouse, transformed_df)

            print(f"Rows written to Clickhouse: {write_count}")

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        print(f"Incremental load finished in {duration} seconds\n")

    finally:
        spark.stop()