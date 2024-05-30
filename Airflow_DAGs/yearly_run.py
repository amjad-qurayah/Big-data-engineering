from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),  # Start date for the annual execution
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "azure_data_factory_conn_id": "azure_data_factory",  # Connection ID for Azure Data Factory
    "resource_group_name": "big-data-project",  # Resource group name
    "factory_name": "big-data-project"  # Azure Data Factory name
}

dag = DAG(
    dag_id="annualpipelines",
    default_args=default_args,
    schedule_interval="@yearly",  # Set the schedule to run each year
    catchup=False
)

def decide_which_pipeline(**kwargs):
    return ["run_QuestionsQuality_pipeline", "run_SurveyData_pipeline"]


branching_task = BranchPythonOperator(
    task_id="branching_task",
    python_callable=decide_which_pipeline,
    provide_context=True,
    dag=dag
)

run_QuestionsQuality_pipeline = AzureDataFactoryRunPipelineOperator(
    task_id="run_QuestionsQuality_pipeline",
    azure_data_factory_conn_id=default_args["azure_data_factory_conn_id"],
    resource_group_name=default_args["resource_group_name"],
    factory_name=default_args["factory_name"],
    pipeline_name="QuestionsQuality",  # Name of the Azure Data Factory pipeline 1
    dag=dag
)

run_SurveyData_pipeline = AzureDataFactoryRunPipelineOperator(
    task_id="run_SurveyData_pipeline",
    azure_data_factory_conn_id=default_args["azure_data_factory_conn_id"],
    resource_group_name=default_args["resource_group_name"],
    factory_name=default_args["factory_name"],
    pipeline_name="SurveyData",  # Name of the Azure Data Factory pipeline 2
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

branching_task >> run_QuestionsQuality_pipeline
branching_task >> run_SurveyData_pipeline
run_QuestionsQuality_pipeline >> end_task
run_SurveyData_pipeline >> end_task
