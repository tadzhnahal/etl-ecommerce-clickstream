import os
from copy import deepcopy
from pathlib import Path

import yaml
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

def load_env() -> None:
    load_dotenv(BASE_DIR / ".env")

def _env(name: str, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value

def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return int(default)
    return int(value)

def apply_env_overrides(raw_config: dict) -> dict:
    resolved = deepcopy(raw_config)

    source = resolved["source"]
    source["host"] = _env("POSTGRES_HOST", source["host"])
    source["port"] = _env_int("POSTGRES_PORT", source["port"])
    source["database"] = _env("POSTGRES_DB", source["database"])
    source["user"] = _env("POSTGRES_USER", source["user"])

    target = resolved["target"]
    target["host"] = _env("CLICKHOUSE_HOST", target["host"])
    target["http_port"] = _env_int("CLICKHOUSE_HTTP_PORT", target["http_port"])
    target["native_port"] = _env_int("CLICKHOUSE_NATIVE_PORT", target["native_port"])
    target["database"] = _env("CLICKHOUSE_DB", target["database"])
    target["user"] = _env("CLICKHOUSE_USER", target["user"])

    hwm_store = resolved["hwm_store"]
    hwm_store["type"] = _env("HWM_STORE_TYPE", hwm_store["type"])
    hwm_store["path"] = _env("HWM_STORE_PATH", hwm_store["path"])

    spark = resolved["spark"]
    spark["app_name"] = _env("SPARK_APP_NAME", spark["app_name"])
    spark["master"] = _env("SPARK_MASTER", spark["master"])

    return resolved

def load_config() -> dict:
    load_env()

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        raw_config = yaml.safe_load(file)

    return apply_env_overrides(raw_config)

config = load_config()