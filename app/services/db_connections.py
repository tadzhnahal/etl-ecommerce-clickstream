import os
from onetl.connection import Clickhouse, Postgres
from pyspark.sql import SparkSession

def get_postgres_connection(spark: SparkSession) -> Postgres:
    return Postgres(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "tadzhnahal"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        database=os.getenv("POSTGRES_DB", "ecommerce"),
        spark=spark,
    )

def get_clickhouse_connection(spark: SparkSession) -> Clickhouse:
    return Clickhouse(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        user=os.getenv("CLICKHOUSE_USER", "tadzhnahal"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "analytics"),
        spark=spark,
    )