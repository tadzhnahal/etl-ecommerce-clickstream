import os
from onetl.connection import Clickhouse, Postgres
from pyspark.sql import SparkSession
from app.core.config import config

def get_postgres_connection(spark: SparkSession) -> Postgres:
    source = config["source"]

    return Postgres(
        host=source["host"],
        port=int(source["port"]),
        user=source["user"],
        password=os.getenv("POSTGRES_PASSWORD", ""),
        database=source["database"],
        spark=spark,
    )

def get_clickhouse_connection(spark: SparkSession) -> Clickhouse:
    target = config["target"]

    return Clickhouse(
        host=target["host"],
        port=int(target["http_port"]),
        user=target["user"],
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=target["database"],
        spark=spark,
    )