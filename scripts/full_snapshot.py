import os
import time
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

BASE_DIR = Path(__file__).resolve().parent.parent

def load_env() -> None:
    load_dotenv(BASE_DIR / ".env")

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("full_snapshot")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0",
        )
        .getOrCreate()
    )

def read_source_table(spark: SparkSession):
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "ecommerce")
    postgres_user = os.getenv("POSTGRES_USER", "tadzhnahal")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "")

    jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "raw.events")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return df

def transform_clickstream(df):
    before_count = df.count()

    df = df.filter (F.col("event_time").isNotNull())
    df = df.filter(F.col("product_id").isNotNull())

    df = df.withColumn(
        "category_code",
        F.when(
            F.lower(F.trim(F.col("category_code"))) == "nan",
            F.lit(None)
        ).otherwise(F.col("category_code")),
    )

    df = df.withColumn("event_date", F.to_date("event_time"))
    df = df.withColumn("event_hour", F.hour("event_time"))
    df = df.withColumn("day_of_week", F.dayofweek("event_time"))

    category_parts = F.split(F.col("category_code"), r"\.")

    df = df.withColumn("category_level_1", F.get(category_parts, 0))
    df = df.withColumn("category_level_2", F.get(category_parts, 1))
    df = df.withColumn("category_level_3", F.get(category_parts, 2))

    after_count = df.count()

    return df, before_count, after_count

def truncate_clickhouse_table(spark: SparkSession) -> None:
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_http_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    clickhouse_db = os.getenv("CLICKHOUSE_DB", "analytics")
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "tadzhnahal")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

    jdbc_url = f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_http_port}/{clickhouse_db}"

    query = "truncate table analytics.dm_events_clean"

    (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", query)
        .option("user", clickhouse_user)
        .option("password", clickhouse_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
    )

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
    spark = get_spark()

    try:
        print("\nStart full_snapshot\n")

        source_df = read_source_table(spark)
        transformed_df, before_count, after_count = transform_clickstream(source_df)

        print(f"Rows before filtering: {before_count}")
        print(f"Rows after filtering: {after_count}")

        write_count = write_to_clickhouse(transformed_df)

        print(f"Rows to ClickHouse: {write_count}")

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        print(f"Full snapshot finished in {duration} seconds\n")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

