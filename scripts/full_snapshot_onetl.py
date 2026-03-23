import os
import time
from app.core.config import load_env
from app.core.spark import get_spark
from app.services.transforms import transform_clickstream

from onetl.connection import Postgres, Clickhouse
from onetl.db import DBReader, DBWriter

def get_postgres_connection(spark):
    postgres = Postgres(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "tadzhnahal"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        database=os.getenv("POSTGRES_DB", "ecommerce"),
        spark=spark,
    )

    return postgres

def get_clickhouse_connection(spark):
    clickhouse = Clickhouse(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        user=os.getenv("CLICKHOUSE_USER", "tadzhnahal"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "analytics"),
        spark=spark,
    )

    return clickhouse

def read_source_table_onetl(postgres):
    reader = DBReader(
        connection=postgres,
        source="raw.events",
    )

    df = reader.run()
    return df

def truncate_clickhouse_table(clickhouse):
    clickhouse.execute("truncate table analytics.dm_events_clean")

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
        target="analytics.dm_events_clean",
        options=Clickhouse.WriteOptions(
            if_exists="append",
        ),
    )

    writer.run(result_df)
    return write_count

def main() -> None:
    start_time = time.time()

    load_env()

    spark = get_spark(
        app_name="full_snapshot_onetl",
        jars_packages="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0",
    )

    try:
        print("\nStart full snapshot via onETL\n")

        postgres = get_postgres_connection(spark)
        clickhouse = get_clickhouse_connection(spark)

        source_df = read_source_table_onetl(postgres)
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

if __name__ == "__main__":
    main()