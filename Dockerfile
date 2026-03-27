from spark:python3

user root

workdir /opt/project

env pythondontwritebytecode=1
env pythonunbuffered=1
env pythonpath=/opt/project

copy requirements.txt /tmp/requirements.txt

run pip install --no-cache-dir -r /tmp/requirements.txt

copy app /opt/project/app
copy config /opt/project/config
copy scripts /opt/project/scripts
copy sql /opt/project/sql

user spark

cmd ["python", "-m", "app.jobs.spark_full_job"]
