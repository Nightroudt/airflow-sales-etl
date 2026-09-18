FROM apache/airflow:2.10.3-python3.12

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
# Same constraints file used for local dev — keeps the dependency
# resolution identical in Docker instead of letting pip re-resolve freely
# on top of the base image's already-installed set.
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"

COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
COPY etl/ /opt/airflow/etl/
COPY db/ /opt/airflow/db/
COPY alembic/ /opt/airflow/alembic/
COPY alembic.ini /opt/airflow/alembic.ini

# Our own modules (etl, db, plugins) are imported as top-level packages —
# /opt/airflow is already the working directory Airflow itself runs from,
# but isn't on sys.path by default for arbitrary imports, so make it explicit.
ENV PYTHONPATH=/opt/airflow
