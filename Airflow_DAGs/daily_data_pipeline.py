from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.utils.edgemodifier import Label
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="daily_data_pipeline",
    start_date=datetime(2024, 5, 21),
    schedule_interval="0 2 * * *", #run daily at 2:00 AM
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "azure_data_factory",
        "factory_name": "big-data-project",
        "resource_group_name": "big-data-project",
    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    # Define pipeline tasks
    run_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline",
        pipeline_name="copyPostsEveryday",
        
    )

    begin >> Label("Daily Processing") >> run_pipeline >> end
