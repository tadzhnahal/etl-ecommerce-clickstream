import json
from celery.result import AsyncResult
from app.celery_app import celery_app

HISTORY_KEY = "etl_task_history"

def save_task_to_history(task_id: str, task_type: str) -> None:
    client = celery_app.backend.client

    item = {
        "task_id": task_id,
        "task_type": task_type,
    }

    client.lpush(HISTORY_KEY, json.dumps(item))
    client.ltrim(HISTORY_KEY, 0, 49)

def get_task_history(limit: int = 20) -> list[dict]:
    client = celery_app.backend.client
    raw_items = client.lrange(HISTORY_KEY, 0, limit - 1)

    history = []

    for raw_item in raw_items:
        decoded_item = raw_item.decode("utf-8")

        try:
            item = json.loads(decoded_item)

            if isinstance(item, str):
                item = {
                    "task_id": item,
                    "task_type": None,
                }

        except json.JSONDecodeError:
            item = {
                "task_id": decoded_item,
                "task_type": None,
            }

        result = AsyncResult(item["task_id"], app=celery_app)

        history_item = {
            "task_id": item["task_id"],
            "task_type": item["task_type"],
            "status": result.status,
            "result": result.result if result.successful() else None,
            "error": str(result.result) if result.failed() else None,
        }

        history.append(history_item)

    return history

def task_exists_in_history(task_id: str) -> bool:
    client = celery_app.backend.client
    raw_items = client.lrange(HISTORY_KEY, 0, 49)

    for raw_item in raw_items:
        decoded_item = raw_item.decode("utf-8")

        try:
            item = json.loads(decoded_item)

            if isinstance(item, str):
                current_task_id = item
            else:
                current_task_id = item["task_id"]

        except json.JSONDecodeError:
            current_task_id = decoded_item

        if current_task_id == task_id:
            return True

    return False