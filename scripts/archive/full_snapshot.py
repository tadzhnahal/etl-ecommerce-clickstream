import os
import time
import clickhouse_connect
from app.core.config import load_env
from app.core.spark import get_spark
from app.services.source_reader import read_source_table
from app.services.transforms import transform_clickstream

def truncate_clickhouse_table() -> None:
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_http_port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    clickhouse_db = os.getenv("CLICKHOUSE_DB", "analytics")
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "tadzhnahal")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

    client = clickhouse_connect.get_client(
        host=clickhouse_host,
        port=clickhouse_http_port,
        username=clickhouse_user,
        password=clickhouse_password,
        database=clickhouse_db,
    )

    client.command("truncate table dm_events_clean")
    client.close()

def write_to_clickhouse(df):
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_http_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    clickhouse_db = os.getenv("CLICKHOUSE_DB", "analytics")
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "tadzhnahal")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

    jdbc_url = f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_http_port}/{clickhouse_db}"

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

    (
        result_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dm_events_clean")
        .option("user", clickhouse_user)
        .option("password", clickhouse_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )

    return write_count

def main() -> None:
    start_time = time.time()

    load_env()
    spark = get_spark(
        app_name="full_snapshot",
        jars_packages="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0",
    )

    try:
        print("\nStart full snapshot\n")

        source_df = read_source_table(spark)
        transformed_df, before_count, after_count = transform_clickstream(source_df)

        print(f"Rows before filtering: {before_count}")
        print(f"Rows after filtering: {after_count}")

        print("Clear Clickhouse target table")
        truncate_clickhouse_table()

        write_count = write_to_clickhouse(transformed_df)

        print(f"Rows to Clickchouse: {write_count}")

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        print(f"Full snapshot finished in {duration} seconds\n")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

