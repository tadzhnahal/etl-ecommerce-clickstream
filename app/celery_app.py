from celery import Celery

celery_app = Celery(
    "etl_tasks",
    broker_url="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
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