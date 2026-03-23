import pyspark.sql.functions as F

def transform_clickstream(df):
    before_count = df.count()

    df = df.filter(F.col("event_time").isNotNull())
    df = df.filter(F.col("product_id").isNotNull())

    df = df.withColumn(
        "category_code",
        F.when(
            F.lower(F.trim(F.col("category_code"))) == "nan",
            F.lit(None)
        ).otherwise(F.col("category_code"))
    )

    df = df.withColumn("event_date", F.to_date("event_time"))
    df = df.withColumn("event_hour", F.hour("event_time"))
    df = df.withColumn("day_of_week", F.dayofweek("event_time"))

    category_parts = F.split(F.col("category_code"), r"\.")

    df = df.withColumn("category_level_1", F.get(category_parts, 0))
    df = df.withColumn("category_level_2", F.get(category_parts, 1))
    df = df.withColumn("category_level_3", F.get(category_parts, 2))

    after_count = df.count()

    return df, before_count, after_count