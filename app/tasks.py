from app.celery_app import celery_app
from app.services.etl_jobs import run_full_snapshot_onetl, run_incremental_onetl

@celery_app.task(name="app.tasks.ping_task")
def ping_task():
    return {"status": "ok", "message": "Celery works"}

@celery_app.task(name="app.tasks.run_full_snapshot_task")
def run_full_snapshot_task():
    return run_full_snapshot_onetl()

@celery_app.task(name="app.tasks.run_incremental_task")
def run_incremental_task():
    return run_incremental_onetl()