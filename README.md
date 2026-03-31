# Проект по созданию ETL-пайплайна с Apache Spark и onETL для дисциплины «Системы обработки больших данных на Python» МНАД ФКН ВШЭ

## О проекте

Этот репозиторий содержит ETL-сервис для данных интернет-магазина. Сервис загружает исходные события из датасета Kaggle в PostgreSQL, читает их через Apache Spark и onETL, очищает и дополняет данные, а затем записывает результат в ClickHouse. Проект поддерживает полный проход по всей таблице источника и инкрементальный проход только по новым событиям. Для инкрементального режима сервис хранит HWM в YAML-файле внутри репозитория.

Проект также включает HTTP API на FastAPI и фоновые задачи на Celery и Redis. Для локального сценария используется Docker Compose. Также проект содержит Dockerfile, Kubernetes-манифесты и запускает Spark job в Minikube.

## Стек

Python, PostgreSQL, ClickHouse, Apache Spark, onETL, FastAPI, Celery, Redis, Docker, Docker Compose, Minikube.

## Структура репозитория

Основной код лежит в папке `app`. Конфигурация лежит в папке `config`. SQL-скрипты лежат в папке `sql`. Скрипт загрузки датасета лежит в `scripts/load_kaggle_data.py`. Точки входа для Spark job лежат в `app/jobs`. Kubernetes-манифесты лежат в папке `k8s`. Папка `scripts/archive` хранит старые и вспомогательные скрипты и не участвует в основном маршруте запуска. Папка `state` хранит YAML-файл с HWM во время инкрементальной загрузки.

```text
.
├── app
│   ├── celery_app.py
│   ├── core
│   │   ├── config.py
│   │   └── spark.py
│   ├── jobs
│   │   ├── spark_full_job.py
│   │   └── spark_incremental_job.py
│   ├── main.py
│   ├── run_api.py
│   ├── services
│   │   ├── db_connections.py
│   │   ├── etl_jobs.py
│   │   ├── source_reader.py
│   │   ├── task_history.py
│   │   └── transforms.py
│   └── tasks.py
├── config
│   ├── config.yaml
│   └── spark-defaults.conf
├── data
│   └── raw
├── docker-compose.yml
├── Dockerfile
├── k8s
│   ├── driver-pod-template.yaml
│   ├── namespace.yaml
│   ├── rbac.yaml
│   ├── serviceaccount.yaml
│   ├── spark-configmap.yaml
│   ├── spark-secret.example.yaml
│   ├── spark-submit-job-files-configmap.yaml
│   └── spark-submit-job.yaml
├── Makefile
├── README.md
├── requirements.txt
├── scripts
│   ├── archive
│   └── load_kaggle_data.py
├── sql
│   ├── clickhouse
│   │   └── create_events_clean.sql
│   └── postgres
│       ├── create_schema_raw.sql
│       └── create_table_events.sql
└── state
    └── yml_hwm_store
```

## Конфигурация

Базовая конфигурация лежит в файле `config/config.yaml`. Локальный сценарий берет адреса, порты, имена баз и пользователей из `.env`, если вы укажете их там. Kubernetes-сценарий берет те же значения из `ConfigMap` и `Secret`, которые попадут в контейнер как переменные окружения. Эта схема позволяет использовать один и тот же `config.yaml` для локального и Kubernetes-сценария без ручной правки файла перед каждым запуском.

Шаблон переменных окружения лежит в файле `.env.example`. Для локального сценария вам достаточно указать пароль PostgreSQL, пароль ClickHouse и токен Kaggle. Остальные значения уже лежат в шаблоне и подходят для стандартного запуска.

## API

Сервис поднимает HTTP API на FastAPI. FastAPI автоматически публикует Swagger UI по адресу `http://127.0.0.1:8000/docs`.

API содержит такие маршруты:

* `GET /` — возвращает базовый ответ сервиса.
* `POST /etl/full` — ставит в очередь задачу полного прохода.
* `POST /etl/incremental` — ставит в очередь задачу инкрементального прохода.
* `GET /etl/status/{task_id}` — возвращает статус конкретной задачи.
* `GET /etl/history` — возвращает историю последних задач.

## Установка и запуск

### 1. Подготовьте окружение

Установите Python 3.10 или новее. Установите Java. Установите Docker и Docker Compose. Для Kubernetes-сценария установите `kubectl` и Minikube.

Проверьте Java. Выполните команду:

```bash
java -version
```

Склонируйте репозиторий и перейдите в его корень. Выполните команду:

```bash
git clone https://github.com/tadzhnahal/etl-ecommerce-clickstream
cd etl-ecommerce-clickstream
```

### 2. Создайте виртуальное окружение и установите зависимости

Создайте виртуальное окружение. Активируйте его. Установите зависимости из `requirements.txt`.

Выполните команду:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

При желании проверьте модули проекта на синтаксические ошибки. Выполните команду:

```bash
python -m compileall app scripts
```

### 3. Подготовьте файл `.env`

Скопируйте шаблон переменных окружения в рабочий файл. Выполните команду:

```bash
cp .env.example .env
```

Откройте файл `.env` и укажите реальные значения для паролей и токена Kaggle. Оставьте остальные значения без изменений. Используйте такой шаблон:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce
POSTGRES_USER=tadzhnahal
POSTGRES_PASSWORD=your_pg_password

CLICKHOUSE_HOST=localhost
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_NATIVE_PORT=9000
CLICKHOUSE_DB=analytics
CLICKHOUSE_USER=tadzhnahal
CLICKHOUSE_PASSWORD=your_ch_password

REDIS_HOST=localhost
REDIS_PORT=6379

KAGGLE_API_TOKEN=your_kaggle_api_token

HWM_STORE_TYPE=yaml
HWM_STORE_PATH=./state/yml_hwm_store

SPARK_APP_NAME=etl-ecommerce-clickstream
SPARK_MASTER=local[*]
SPARK_CONF_DIR=./config
```

После этого загрузите переменные окружения в текущую shell-сессию. Выполните команду:

```bash
set -a
source .env
set +a
```

### 4. Поднимите локальную инфраструктуру

Запустите PostgreSQL, ClickHouse и Redis через Docker Compose. Выполните команду:

```bash
docker compose up -d
```

После этого посмотрите список контейнеров. Выполните команду:

```bash
docker ps
```

### 5. Подготовьте ClickHouse

Сначала создайте базу `analytics`. Затем создайте целевую таблицу `dm_events_clean` из SQL-скрипта проекта.

Создайте базу. Выполните команду:

```bash
docker exec -it etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "create database if not exists analytics"
```

Создайте таблицу. Выполните команду:

```bash
docker exec -i etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --multiquery < sql/clickhouse/create_events_clean.sql
```

После этого посмотрите список таблиц в базе `analytics`. Выполните команду:

```bash
docker exec -it etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "show tables from analytics"
```

### 6. Загрузите исходные данные в PostgreSQL

Запустите загрузчик данных. Скрипт скачает датасет из Kaggle, создаст схему `raw`, создаст таблицу `raw.events` и загрузит строки в PostgreSQL.

Для базового маршрута загрузите `100001` строку. Выполните команду:

```bash
python -m scripts.load_kaggle_data --limit 100001
```

После этого посмотрите число строк в исходной таблице. Выполните команду:

```bash
docker exec -it etl_postgres psql \
  -U tadzhnahal \
  -d ecommerce \
  -c "select count(*) from raw.events;"
```

Если захотите добавить новые строки без очистки исходной таблицы, запустите загрузчик с флагом `--append`. Выполните команду:

```bash
python -m scripts.load_kaggle_data --limit 100001 --append
```

### 7. Запустите full snapshot

Перед первым запуском очистите HWM и целевую таблицу ClickHouse. Этот шаг даст чистое состояние для полного прохода.

Удалите HWM. Выполните команду:

```bash
rm -rf state/yml_hwm_store
```

Очистите целевую таблицу. Выполните команду:

```bash
docker exec -it etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "truncate table analytics.dm_events_clean"
```

После этого запустите full snapshot. Выполните команду:

```bash
python -m app.jobs.spark_full_job
```

Затем посмотрите число строк в ClickHouse. Выполните команду:

```bash
docker exec -it etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "select count() from analytics.dm_events_clean"
```

### 8. Запустите incremental load

Если хотите запустить первый инкремент с нуля, снова очистите HWM и целевую таблицу.

Удалите HWM. Выполните команду:

```bash
rm -rf state/yml_hwm_store
```

Очистите целевую таблицу. Выполните команду:

```bash
docker exec -it etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "truncate table analytics.dm_events_clean"
```

После этого запустите incremental load первый раз. Выполните команду:

```bash
python -m app.jobs.spark_incremental_job
```

Затем посмотрите, какие файлы появились в папке `state`. Выполните команду:

```bash
find state -maxdepth 3 -type f | sort
```

После этого откройте HWM-файл. Выполните команду:

```bash
cat state/yml_hwm_store/clickstream_event_time_hwm.yml
```

Теперь снова запустите incremental load. Второй запуск считает только события после последней сохраненной метки. Выполните команду:

```bash
python -m app.jobs.spark_incremental_job
```

### 9. Добавьте новые данные и снова запустите incremental load

Если хотите показать настоящий инкремент, добавьте новую строку в PostgreSQL вручную.

Выполните запрос:

```bash
docker exec -it etl_postgres psql \
  -U tadzhnahal \
  -d ecommerce \
  -c "
insert into raw.events (
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
) values (
    now(),
    'view',
    888888888,
    1,
    'manual.test.category',
    'manual_brand',
    99.99,
    123456789,
    'manual-session-hwm-test'
);
"
```

После этого снова запустите incremental load. Выполните команду:

```bash
python -m app.jobs.spark_incremental_job
```

Затем посмотрите итоговое число строк в ClickHouse. Выполните команду:

```bash
docker exec -it etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "select count() from analytics.dm_events_clean"
```

### 10. Запустите API

Откройте новый терминал. Перейдите в корень репозитория. Активируйте виртуальное окружение. После этого запустите API.

Выполните команду:

```bash
source .venv/bin/activate
python -m app.run_api
```

После этого откройте Swagger UI по адресу `http://127.0.0.1:8000/docs`.

### 11. Запустите Celery worker

Откройте еще один терминал. Перейдите в корень репозитория. Активируйте виртуальное окружение. После этого запустите worker.

Выполните команду:

```bash
source .venv/bin/activate
celery -A app.tasks worker --loglevel=info --pool=solo
```

### 12. Вызовите API вручную

Сначала обратитесь к корневому маршруту. Выполните команду:

```bash
curl -s http://127.0.0.1:8000/
```

После этого поставьте в очередь full snapshot. Выполните команду:

```bash
curl -s -X POST http://127.0.0.1:8000/etl/full
```

Затем поставьте в очередь incremental load. Выполните команду:

```bash
curl -s -X POST http://127.0.0.1:8000/etl/incremental
```

Если захотите получить статус задачи, подставьте `task_id` из ответа и запросите статус. Выполните команду:

```bash
curl -s http://127.0.0.1:8000/etl/status/<task_id>
```

Если захотите получить историю задач, запросите историю. Выполните команду:

```bash
curl -s http://127.0.0.1:8000/etl/history
```

## Запуск Spark job в Kubernetes

### 13. Подготовьте секреты для Kubernetes

Скопируйте шаблон секрета в рабочий файл. Выполните команду:

```bash
cp k8s/spark-secret.example.yaml k8s/spark-secret.yaml
```

Откройте `k8s/spark-secret.yaml` и укажите реальные пароли для PostgreSQL и ClickHouse.

### 14. Поднимите Minikube

Запустите Minikube. Выполните команду:

```bash
minikube start
```

### 15. Соберите Docker-образ внутри Minikube

Соберите образ Spark-приложения прямо в окружении Minikube. Выполните команду:

```bash
minikube image build -t etl-clickstream-spark:dev .
```

### 16. Примените Kubernetes-манифесты

Перед новым запуском удалите старый namespace, если он уже существует. Выполните команду:

```bash
kubectl delete namespace spark-jobs --ignore-not-found
kubectl wait --for=delete namespace/spark-jobs --timeout=120s 2>/dev/null || true
```

После этого примените манифесты по порядку.

Сначала создайте namespace. Выполните команду:

```bash
kubectl apply -f k8s/namespace.yaml
```

Затем создайте service account. Выполните команду:

```bash
kubectl apply -f k8s/serviceaccount.yaml
```

После этого примените RBAC-манифест. Выполните команду:

```bash
kubectl apply -f k8s/rbac.yaml
```

Затем создайте ConfigMap с окружением для Spark job. Выполните команду:

```bash
kubectl apply -f k8s/spark-configmap.yaml
```

После этого создайте Secret с паролями. Выполните команду:

```bash
kubectl apply -f k8s/spark-secret.yaml
```

Затем создайте ConfigMap с шаблоном driver pod. Выполните команду:

```bash
kubectl apply -f k8s/spark-submit-job-files-configmap.yaml
```

В конце создайте Spark submit job. Выполните команду:

```bash
kubectl apply -f k8s/spark-submit-job.yaml
```

Если хотите посмотреть права service account, выполните команду:

```bash
kubectl auth can-i create pods --as=system:serviceaccount:spark-jobs:spark-driver -n spark-jobs
kubectl auth can-i create services --as=system:serviceaccount:spark-jobs:spark-driver -n spark-jobs
kubectl auth can-i create configmaps --as=system:serviceaccount:spark-jobs:spark-driver -n spark-jobs
```

### 17. Подготовьте ClickHouse перед Kubernetes-запуском

Если хотите получить чистый результат, очистите целевую таблицу перед запуском Spark job в Kubernetes. Выполните команду:

```bash
docker exec -it etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "truncate table analytics.dm_events_clean"
```

### 18. Запустите Spark job в Kubernetes

Сначала посмотрите статус job. Выполните команду:

```bash
kubectl get jobs -n spark-jobs
```

После этого посмотрите pod'ы. Выполните команду:

```bash
kubectl get pods -n spark-jobs
```

Затем откройте логи `spark-submit`. Выполните команду:

```bash
kubectl logs -n spark-jobs job/spark-submit-job
```

Если хотите посмотреть переходы статусов в реальном времени, откройте наблюдение за pod'ами. Выполните команду:

```bash
kubectl get pods -n spark-jobs -w
```

После завершения job посмотрите число строк в ClickHouse. Выполните команду:

```bash
docker exec -it etl_clickhouse clickhouse-client \
  --user tadzhnahal \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "select count() from analytics.dm_events_clean"
```

Текущий Kubernetes-манифест запускает только full snapshot. Для Kubernetes-сценария репозиторий не содержит отдельный job-манифест для incremental load.

## Остановка сервисов

Когда закончите локальный сценарий, остановите API и Celery через `Ctrl+C`. После этого остановите контейнеры Docker Compose. Выполните команду:

```bash
docker compose down
```

Если захотите остановить Minikube, выполните команду:

```bash
minikube stop
```
