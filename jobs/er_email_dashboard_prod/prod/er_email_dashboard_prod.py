from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from p2_retargeting.lib.email_utils import Email
from p2_retargeting.lib.databricks_factory.p2_databricks_factory import databricks_job_workflow

DAG_ID: str = "er_email_dashboard_prod"
DAG_VARS: dict = Variable.get(DAG_ID, deserialize_json=True)
ALERT_EMAILS: str = DAG_VARS.get("alert_emails")

STACK: str = DAG_VARS.get("stack")
EMAIL: Email = Email(recipient_list=ALERT_EMAILS, stack=STACK)

default_args: dict = {
    "owner": "p2-cleanroom",
    "depends_on_past": False,
    "on_failure_callback": EMAIL.error,
}

access_control: dict = {"p2-retargeting-hp": ["can_read", "can_edit"]}

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    access_control=access_control,
) as dag:
    FINISHED_TASK = DummyOperator(
        dag=dag,
        task_id="FINISHED",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    er_paas_email_mapping: TaskGroup = databricks_job_workflow(
        dag=dag,
        dag_vars=DAG_VARS,
        downstream_task=FINISHED_TASK,
        job_key="ER_EMAIL_DASHBOARD_PROD_JOB_KEY",
        polling_duration="60",
        email=EMAIL,
        service_account_name="p2-cleanroom-airflow-sa",
    )
