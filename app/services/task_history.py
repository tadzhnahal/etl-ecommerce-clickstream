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
        item = json.loads(raw_item.decode("utf-8"))
        result = AsyncResult(item["task_id"], app=celery_app)

        history_item = {
            "task_id": item["task_id"],
            "task_type": item["task_type"],
            "status": result.status,
            "result": result.result if result.successful() else None,
        }

        if result.failed():
            history_item["error"] = str(result.result)

        history.append(history_item)

    return history