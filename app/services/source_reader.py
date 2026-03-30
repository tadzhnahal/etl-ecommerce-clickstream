import os
from pyspark.sql import SparkSession
from app.core.config import config

def read_source_table(spark: SparkSession):
    source = config["source"]
    source_table = f"{source['schema']}.{source['table']}"

    jdbc_url = f"jdbc:postgresql://{source['host']}:{source['port']}/{source['database']}"

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", source_table)
        .option("user", source["user"])
        .option("password", os.getenv("POSTGRES_PASSWORD", ""))
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return df