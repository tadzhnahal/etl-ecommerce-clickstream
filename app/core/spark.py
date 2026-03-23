from pyspark.sql import SparkSession

def get_spark(app_name: str, jars_packages: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", jars_packages)
        .getOrCreate()
    )