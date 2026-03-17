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
        .appName("transform_clickstream")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
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

    # убираем строки без времени события
    df = df.filter (F.col("event_time").isNotNull())

    # убираем строки без product_id
    df = df.filter(F.col("product_id").isNotNull())

    # добавляем простые временные поля
    df = df.withColumn("event_date", F.to_date("event_time"))
    df = df.withColumn("event_hour", F.hour("event_time"))
    df = df.withColumn("day_of_week", F.dayofweek("event_time"))

    # делим на уровни
    category_parts = F.split(F.col("category_code"), r"\.")

    df = df.withColumn("category_level_1", F.get(category_parts, 0))
    df = df.withColumn("category_level_2", F.get(category_parts, 1))
    df = df.withColumn("category_level_3", F.get(category_parts, 2))

    after_count = df.count()

    return df, before_count, after_count

def main() -> None:
    load_env()
    spark = get_spark()

    try:
        df = read_source_table(spark)
        transformed_df, before_count, after_count = transform_clickstream(df)

        print("\nRows before filtering:\n")
        print(before_count)

        print("\nRows after filtering:\n")
        print(after_count)

        print("\nSchema after transformation:\n")
        transformed_df.printSchema()

        print("\nFirst 10 rows after transformation:\n")
        transformed_df.select(
            "event_time",
            "event_type",
            "product_id",
            "category_code",
            "category_level_1",
            "category_level_2",
            "category_level_3",
            "event_date",
            "event_hour",
            "day_of_week",
            "price",
            "user_id",
        ).show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()