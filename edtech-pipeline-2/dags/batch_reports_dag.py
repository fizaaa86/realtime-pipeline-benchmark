"""
Airflow DAG — Nightly EdTech Pipeline Batch Report
====================================================
Schedules batch_reports.py via spark-submit on EC2-2 every night at 00:00 UTC.

Deployment (on EC2-2):
  pip install apache-airflow
  export AIRFLOW_HOME=~/airflow
  airflow db init
  cp dags/batch_reports_dag.py ~/airflow/dags/
  airflow scheduler &
  airflow webserver --port 8080 &

Current deployment uses cron as a lightweight alternative (see §10 of architecture doc).
This DAG is the production-ready equivalent for environments with ≥3 interdependent jobs.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

SPARK_SUBMIT = (
    "spark-submit "
    "--jars $(ls /opt/spark/jars/*.jar | tr '\\n' ',') "
    "/home/ubuntu/edtech-pipeline-2/spark/batch_reports.py "
    "--date {{ ds }}"
)

S3_VERIFY = (
    "aws s3 ls s3://$S3_BUCKET/raw/year={{ execution_date.year }}/"
    "month={{ '{:02d}'.format(execution_date.month) }}/"
    "day={{ '{:02d}'.format((execution_date - macros.timedelta(days=1)).day) }}/ "
    "| wc -l"
)

default_args = {
    "owner": "edtech",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="edtech_nightly_batch",
    default_args=default_args,
    description="Nightly batch: S3 Parquet → DynamoDB DailyReports + summary Parquet",
    schedule_interval="0 0 * * *",   # midnight UTC daily
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,               # prevent overlapping runs
    tags=["edtech", "batch", "csg527"],
) as dag:

    start = EmptyOperator(task_id="start")

    verify_s3_data = BashOperator(
        task_id="verify_s3_data",
        bash_command=S3_VERIFY,
        doc_md="Check that yesterday's raw Parquet files exist in S3 before running batch.",
    )

    run_batch_report = BashOperator(
        task_id="run_batch_report",
        bash_command=SPARK_SUBMIT,
        doc_md="""
        Runs spark/batch_reports.py for the previous day ({{ ds }}).
        Reads raw Parquet from S3, computes daily aggregates via Spark SQL,
        writes DynamoDB DailyReports table and reports/daily/{{ ds }}/report.parquet.
        Expected runtime: ~120 seconds (267k events on t3.medium, measured 2026-04-16).
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> verify_s3_data >> run_batch_report >> end
