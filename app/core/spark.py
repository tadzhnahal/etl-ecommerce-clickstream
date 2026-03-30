from pyspark.sql import SparkSession

def get_spark(
    app_name: str,
    jars_packages: str,
    master: str | None = None,
) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", jars_packages)
    )

    if master:
        builder = builder.master(master)

    return builder.getOrCreate()