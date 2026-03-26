from fastapi import FastAPI
from celery.result import AsyncResult
from app.celery_app import celery_app
from app.tasks import run_full_snapshot_task, run_incremental_task

app = FastAPI(title="ETL Clickstream API")

@app.get("/")
def root():
    return {
        "message": "ETL Clickstream API works",
    }

@app.post("/etl/full")
def run_full_etl():
    task = run_full_snapshot_task.delay()

    return {
        "task_id": task.id,
        "task_type": "full_snapshot",
        "status": "queued",
    }

@app.post("/etl/incremental")
def run_incremental_etl():
    task = run_incremental_task.delay()

    return {
        "task_id": task.id,
        "task_type": "incremental",
        "status": "queued",
    }

@app.get("/etl/status/{task_id}")
def get_etl_status(task_id: str):
    result = AsyncResult(task_id, app=celery_app)

    response = {
        "task_id": task_id,
        "status": result.status,
    }

    if result.status == "SUCCESS":
        response["result"] = result.result
    elif result.status == "FAILURE":
        response["error"] = str(result.result)

    return response
