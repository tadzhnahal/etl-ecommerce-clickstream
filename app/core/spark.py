from pyspark.sql import SparkSession
from app.core.config import config

def get_spark(
    app_name: str,
    jars_packages: list[str] | None = None,
    master: str | None = None,
) -> SparkSession:
    spark_config = config["spark"]
    resolved_jars = jars_packages or spark_config["jars_packages"]
    jars_packages_string = ",".join(resolved_jars)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master or spark_config["master"])
        .config("spark.jars.packages", jars_packages_string)
        .config("spark.sql.session.timeZone", spark_config["timezone"])
        .config("spark.sql.shuffle.partitions", str(spark_config["shuffle_partitions"]))
    )

    return builder.getOrCreate()