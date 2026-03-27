from spark:python3

user root

workdir /tmp

env pythondontwritebytecode=1
env pythonunbuffered=1
env pythonpath=/opt/project

copy requirements.txt /tmp/requirements.txt
run pip install --no-cache-dir -r /tmp/requirements.txt

run mkdir -p /opt/project && chown -R spark:spark /opt/project && chmod -R 775 /opt/project

copy app /opt/project/app
copy config /opt/project/config
copy scripts /opt/project/scripts
copy sql /opt/project/sql

run chown -R spark:spark /opt/project && chmod -R 775 /opt/project

user spark

cmd ["python3", "-m", "app.jobs.spark_full_job"]