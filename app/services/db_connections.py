import os
from onetl.connection import Postgres, Clickhouse

def get_postgres_connection(spark):
    return Postgres(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "tadzhnahal"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        database=os.getenv("POSTGRES_DB", "ecommerce"),
        spark=spark
    )

def get_clickhouse_connection(spark):
    return Clickhouse(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        user=os.getenv("CLICKHOUSE_USER", "tadzhnahal"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "analytics"),
        spark=spark
    )