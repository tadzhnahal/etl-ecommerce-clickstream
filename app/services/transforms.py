import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

max_events_per_minute = 30
max_events_per_session = 500

def transform_clickstream(df: DataFrame) -> tuple[DataFrame, int, int]:
    before_count = df.count()

    df = df.filter(F.col("event_time").isNotNull())
    df = df.filter(F.col("product_id").isNotNull())

    df = df.withColumn(
        "category_code",
        F.when(
            F.lower(F.trim(F.col("category_code"))) == "nan",
            F.lit(None),
        ).otherwise(F.col("category_code")),
    )

    df = df.withColumn("event_date", F.to_date("event_time"))
    df = df.withColumn("event_hour", F.hour("event_time"))
    df = df.withColumn("day_of_week", F.dayofweek("event_time"))

    category_parts = F.split(F.col("category_code"), r"\.")

    df = df.withColumn("category_level_1", F.get(category_parts, 0))
    df = df.withColumn("category_level_2", F.get(category_parts, 1))
    df = df.withColumn("category_level_3", F.get(category_parts, 2))

    df = df.withColumn("event_minute", F.date_trunc("minute", F.col("event_time")))

    user_minute_window = Window.partitionBy("user_id", "event_minute")
    session_window = Window.partitionBy("user_session")

    df = df.withColumn("events_per_minute", F.count("*").over(user_minute_window))
    df = df.withColumn("events_per_session", F.count("*").over(session_window))

    df = df.filter(
        (F.col("events_per_minute") <= max_events_per_minute)
        & (F.col("events_per_session") <= max_events_per_session)
    )

    df = df.drop("event_minute", "events_per_minute", "events_per_session")

    after_count = df.count()

    return df, before_count, after_count