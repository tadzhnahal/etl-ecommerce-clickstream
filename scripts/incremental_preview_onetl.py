import time
import pyspark.sql.functions as F
from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

from app.core.config import load_env
from app.core.spark import get_spark
from app.services.db_connections import get_postgres_connection
from app.services.transforms import transform_clickstream

def main() -> None:
    start_time = time.time()

    load_env()

    spark=get_spark(
        app_name="incremental_preview_onetl",
        jars_packages="org.postgresql:postgresql:42.7.3",
    )

    try:
        print("\nStart incremental preview via onETL\n")

        postgres = get_postgres_connection(spark)

        reader = DBReader(
            connection=postgres,
            source="raw.events",
            columns=[
                "event_time",
                "event_type",
                "product_id",
                "category_id",
                "category_code",
                "brand",
                "price",
                "user_id",
                "user_session",
            ],
            hwm=DBReader.AutoDetectHWM(
                name="clickstream_event_time_hwm",
                expression="event_time",
            ),
            options=Postgres.ReadOptions(
                fetchsize=5000,
            ),
        )

        with IncrementalStrategy():
            source_df = reader.run()

            source_count = source_df.count()
            print(f"Rows read from source: {source_count}")

            if source_count == 0:
                print("No new rows found")
                end_time = time.time()
                duration = round(end_time - start_time, 2)
                print(f"Incremental preview finished in {duration} seconds")
                return

            transformed_df, before_count, after_count = transform_clickstream(source_df)

            print(f"Rows before filtering: {before_count}")
            print(f"Rows after filtering: {after_count}")

            max_hwm_row = transformed_df.select(
                F.max("event_time").alias("max_event_time")
            ).collect()[0]

            new_max_event_time = max_hwm_row["max_event_time"]

            print(f"New max event time: {new_max_event_time}")

            print("\nFirst 10 rows from incremental batch:\n")
            transformed_df.select(
                "event_time",
                "event_type",
                "product_id",
                "category_code",
                "event_date",
                "event_hour",
                "day_of_week",
            ).show(10, truncate=False)

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        print(f"Incremental preview finished in {duration} seconds\n")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()