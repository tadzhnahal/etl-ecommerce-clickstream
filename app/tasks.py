from app.celery_app import celery_app

@celery_app.task
def ping_task():
    return {"status": "ok", "message": "Celery works"}