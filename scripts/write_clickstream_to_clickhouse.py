import os
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
        .appName("write_clickstream_to_clickhouse")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0")
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
    df = df.filter(F.col("event_time").isNotNull())
    df = df.filter(F.col("product_id").isNotNull())

    df = df.withColumn(
        "category_code",
        F.when(
            F.lower(F.trim(F.col("category_code"))) == "nan",
            F.lit(None)
        ).otherwise(F.col("category_code"))
    )

    df = df.withColumn("event_date", F.to_date("event_time"))
    df = df.withColumn("event_hour", F.hour("event_time"))
    df = df.withColumn("day_of_week", F.dayofweek("event_time"))

    category_parts = F.split(F.col("category_code"), r"\.")

    df = df.withColumn("category_level_1", F.get(category_parts, 0))
    df = df.withColumn("category_level_2", F.get(category_parts, 1))
    df = df.withColumn("category_level_3", F.get(category_parts, 2))

    return df

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
    spark = get_spark()

    try:
        df = read_source_table(spark)
        transformed_df = transform_clickstream(df)

        print("\nRows to write:\n")
        print(transformed_df.count())

        write_to_clickhouse(transformed_df)

        print("\nWrite finished.\n")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()