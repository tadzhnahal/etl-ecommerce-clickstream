import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent

def load_env() -> None:
    load_dotenv(BASE_DIR / ".env")

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("read_postgres_with_spark")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

def main() -> None:
    load_env()

    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "ecommerce")
    postgres_user = os.getenv("POSTGRES_USER", "tadzhnahal")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "")

    jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

    spark = get_spark()

    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "raw.events")
            .option("user", postgres_user)
            .option("password", postgres_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        print("\nSchema:\n")
        df.printSchema()

        print("\nFirst 5 rows:\n")
        df.show(5, truncate=False)

        print("\nRow count:\n")
        print(df.count())

    finally:
        spark.stop()

if __name__ == "__main__":
    main()