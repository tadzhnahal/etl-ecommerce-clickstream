from celery import Celery
from app.core.config import config

celery_config = config["celery"]

celery_app = Celery(
    "etl_tasks",
    broker=celery_config["broker_url"],
    backend=celery_config["result_backend"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone=celery_config["timezone"],
    enable_utc=True,
    worker_pool=celery_config["worker_pool"],
)

celery_app.autodiscover_tasks(["app"])