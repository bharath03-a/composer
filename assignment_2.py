
import datetime
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago


proj_id = 'manifest-craft-316206'
dataset = 'assignment_1_dataset'
bucket_1 = 'gcp-training-bharath'
bucket_2 = 'archive-buck'

dag = models.DAG(
    dag_id='DAG-2',
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
)


start_pipeline = DummyOperator(
  task_id = 'start_pipeline',
  dag = dag
)

load_data_1 = GCSToBigQueryOperator(
  task_id='gcs_to_bigquery',
  bucket='gcp-training-bharath',
  source_objects=["{{dag_run.conf['name']}}"],
  destination_project_dataset_table= dataset +"."+'{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["TABLE_NAME"] }}',
  autodetect = True,
  write_disposition='WRITE_TRUNCATE',
  skip_leading_rows = 1,
  dag=dag,
)


#creating a transformation of loaded data table
create_transformation = BigQueryExecuteQueryOperator(
  task_id="Table_Transformation",
  sql=str('{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["QUERY"] }}'),
  use_legacy_sql=False,
  destination_dataset_table= dataset +"."+'{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["TABLE"] }}',
  write_disposition='WRITE_TRUNCATE',
  dag=dag,
)

#3 end tasks to be done now
#dropping temp_table in bq
drop_table = BigQueryDeleteTableOperator(
  task_id="drop_table",
  #use_legacy_sql=False,
  deletion_dataset_table=proj_id +"." + dataset +"."+'{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["TABLE_NAME"] }}',
  dag=dag,
)

#archive input file task
archive_file = GCSToGCSOperator(
    task_id="archive_input_file",
    source_bucket=bucket_1,
    source_object="{{ dag_run.conf['name'] }}",
    destination_bucket=bucket_2,  # If not supplied the source_bucket value will be used
    destination_object='{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["ARCHIVE"] }}' + "{{ dag_run.conf['name'] }}",
)

#send the output data to gcs task
bq_to_gcs = BigQueryToGCSOperator(
  task_id="bigquery_to_gcs",
  source_project_dataset_table=dataset+"."+'{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["TABLE"] }}',
  destination_cloud_storage_uris=['{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["BIGQUERY_TO_GCS"] }}'],
  dag=dag,
)

finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)

start_pipeline >> load_data_1 >> create_transformation >> [drop_table, archive_file, bq_to_gcs] >> finish_pipeline