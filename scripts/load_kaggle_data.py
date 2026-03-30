import argparse
import logging
import os
import subprocess
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from app.core.config import config, load_env

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
SQL_DIR = BASE_DIR / "sql" / "postgres"
SOURCE = config["source"]
SOURCE_TABLE = f"{SOURCE['schema']}.{SOURCE['table']}"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)s %(levelname)s - %(message)s',)

logger = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Kaggle clickstream data into Postgres")
    parser.add_argument("--limit", type=int, default=100_001, help="How many rows to load")
    parser.add_argument("--chunk-size", type=int, default=20_000, help="CSV chunk size")
    parser.add_argument("--append", action="store_true", help="Append data to existing table")
    return parser.parse_args()

def get_db_params() -> dict:
    return {
        "host": SOURCE["host"],
        "port": SOURCE["port"],
        "dbname": SOURCE["database"],
        "user": SOURCE["user"],
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }

def run_sql_file(connection, path: Path) -> None:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()

def prepare_database(connection, append: bool) -> None:
    run_sql_file(connection, SQL_DIR / "create_schema_raw.sql")
    run_sql_file(connection, SQL_DIR / "create_table_events.sql")

    if not append:
        logger.info("Clear table raw.events before load")
        with connection.cursor() as cursor:
            cursor.execute(f"truncate table {SOURCE_TABLE};")
        connection.commit()

def download_dataset() -> None:
    dataset = "mkechinov/ecommerce-behavior-data-from-multi-category-store"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Download dataset from Kaggle")
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", dataset, "-p", str(DATA_DIR), "--unzip",],
        check=True,
    )

def find_csv_file() -> Path:
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("CSV file not found in raw")
    return csv_files[0]

def normalize_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    columns = ["event_time", "event_type", "product_id", "category_id",
               "category_code", "brand", "price", "user_id", "user_session",]

    chunk = chunk[columns].copy()

    chunk["event_time"] = pd.to_datetime(chunk["event_time"], errors="coerce", utc=True)
    chunk["event_time"] = chunk["event_time"].dt.tz_convert(None)
    chunk["product_id"] = pd.to_numeric(chunk["product_id"], errors="coerce").astype("Int64")
    chunk["category_id"] = pd.to_numeric(chunk["category_id"], errors="coerce").astype("Int64")
    chunk["price"] = pd.to_numeric(chunk["price"], errors="coerce")
    chunk["user_id"] = pd.to_numeric(chunk["user_id"], errors="coerce").astype("Int64")

    chunk = chunk.where(pd.notnull(chunk), None)
    chunk = chunk.astype(object)

    return chunk

def insert_chunk(connection, chunk: pd.DataFrame) -> int:
    rows = list(chunk.itertuples(index=False, name=None))
    if not rows:
        return 0

    sql = """
            INSERT INTO raw.events (
                event_time,
                event_type,
                product_id,
                category_id,
                category_code,
                brand,
                price,
                user_id,
                user_session
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    with connection.cursor() as cursor:
        execute_batch(cursor, sql, rows, page_size=5000)

    connection.commit()
    return len(rows)

def load_csv_to_postgres(connection, csv_path: Path, limit: int, chunk_size: int) -> int:
    total_loaded = 0

    logger.info("Read CSV file: %s", csv_path.name)

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        chunk = normalize_chunk(chunk)

        remaining = limit - total_loaded
        if remaining <= 0:
            break

        if len(chunk) > remaining:
            chunk = chunk.iloc[:remaining]

        loaded = insert_chunk(connection, chunk)
        total_loaded += loaded

        logger.info("Loaded %s rows, total=%s", loaded, total_loaded)

        if total_loaded >= limit:
            break

    return total_loaded

def main() -> None:
    args = parse_args()
    load_env()

    if not os.getenv("KAGGLE_API_TOKEN"):
        raise ValueError("Set KAGGLE_API_TOKEN in .env")

    connection = psycopg2.connect(**get_db_params())

    try:
        prepare_database(connection, append=args.append)

        csv_files = sorted(DATA_DIR.glob("*.csv"))
        if not csv_files:
            download_dataset()
        else:
            logger.info("Dataset already exists, skipping download")

        csv_path = find_csv_file()
        total_loaded = load_csv_to_postgres(
            connection=connection,
            csv_path=csv_path,
            limit=args.limit,
            chunk_size=args.chunk_size,
        )
        logger.info("Load finished. Total rows in this run: %s", total_loaded)
    finally:
        connection.close()

if __name__ == "__main__":
    main()