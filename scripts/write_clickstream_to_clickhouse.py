import os
from app.core.config import load_env
from app.core.spark import get_spark
from app.services.source_reader import read_source_table
from app.services.transforms import transform_clickstream

def write_to_clickhouse(df):
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    clickhouse_db = os.getenv("CLICKHOUSE_DB", "analytics")
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "tadzhnahal")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

    jdbc_url = f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_db}"

    (
        df.select(
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
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dm_events_clean")
        .option("user", clickhouse_user)
        .option("password", clickhouse_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )

def main() -> None:
    load_env()

    spark = get_spark(
        app_name="write_clickstream_to_clickhouse",
        jars_packages="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0",
    )

    try:
        df = read_source_table(spark)
        transformed_df, before_count, after_count = transform_clickstream(df)

        print("\nRows before filtering:\n")
        print(before_count)

        print("\nRows after filtering:\n")
        print(after_count)

        write_to_clickhouse(transformed_df)

        print("\nWrite finished\n")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()