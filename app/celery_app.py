from celery import Celery
from app.core.settings import load_config

config = load_config()

celery_app = Celery(
    "etl_tasks",
    broker=config["celery"]["broker_url"],
    backend=config["celery"]["result_backend"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Europe/Moscow",
    enable_utc=True,
    worker_pool="solo",
)

celery_app.autodiscover_tasks(["app"])