from app.core.config import load_env
from app.core.spark import get_spark
from app.services.source_reader import read_source_table
from app.services.transforms import transform_clickstream

def main() -> None:
    load_env()

    spark = get_spark(
        app_name="transform_clickstream",
        jars_packages="org.postgresql:postgresql:42.7.3"
    )

    try:
        df = read_source_table(spark)
        transformed_df, before_count, after_count = transform_clickstream(df)

        print("\nRows before filtering:\n")
        print(before_count)

        print("\nRows after filtering:\n")
        print(after_count)

        print("\nSchema after transformation:\n")
        transformed_df.printSchema()

        print("\nFirst 10 rows after transformation:\n")
        transformed_df.select(
            "event_time",
            "event_type",
            "product_id",
            "category_code",
            "category_level_1",
            "category_level_2",
            "category_level_3",
            "event_date",
            "event_hour",
            "day_of_week",
            "price",
            "user_id",
        ).show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()