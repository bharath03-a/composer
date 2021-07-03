import datetime
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator
)



#defining dag variables
proj_id = 'manifest-craft-316206'
bq_data_set = 'assignment_1_dataset'
gs_bucket = 'gcp-training-bharath'
table_name = 'temp_table'
obj = 'data-set-1.csv'
table = 'final_table'


#define dag
dag = models.DAG(
  dag_id='DAG-composer-1',
  start_date=days_ago(2),
  schedule_interval=datetime.timedelta(days=1),
  tags=['example'],
)

#now the taskes


#loading data from GCS to BQ -> using gcstobq operator
start_pipeline = DummyOperator(
  task_id = 'start_pipeline',
  dag = dag
)

load_data_1 = GCSToBigQueryOperator(
  task_id='gcs_to_bigquery',
  bucket='gcp-training-bharath',
  source_objects=['data-set-1.csv'],
  destination_project_dataset_table=f"{bq_data_set}.{table_name}",
  schema_fields=[
 {"name": "invoice_and_item_number", "type": "STRING"},
  {"name": "date", "type": "STRING"},
 {"name": "store_number", "type": "STRING"},
 {"name": "store_name", "type": "STRING"},
 {"name": "address", "type": "STRING"},
 {"name": "city", "type": "STRING"},
 {"name": "zip_code", "type": "STRING"},
 {"name": "store_location", "type": "STRING"},
 {"name": "county_number", "type": "STRING"},
     ],
  write_disposition='WRITE_TRUNCATE',
  dag=dag,
)

#creating a transformation of loaded data table
create_transformation = BigQueryExecuteQueryOperator(
  task_id="Table_Transformation",
  sql=f"SELECT * FROM {bq_data_set}.{table_name} where store_number='2190'",
  use_legacy_sql=False,
  destination_dataset_table=f"{bq_data_set}.{table}",
  write_disposition='WRITE_TRUNCATE',
  dag=dag,
)

#3 end tasks to be done now
#dropping temp_table in bq
drop_table = BigQueryDeleteTableOperator(
  task_id="drop_table",
  #use_legacy_sql=False,
  deletion_dataset_table=f"{proj_id}.{bq_data_set}.{table_name}",
  dag=dag,
)

#archive input file task
archive_file = GCSToGCSOperator(
    task_id="archive_input_file",
    source_bucket=gs_bucket,
    source_object=obj,
    destination_bucket=gs_bucket,  # If not supplied the source_bucket value will be used
    destination_object="backup_" + obj,
)

#send the output data to gcs task
bq_to_gcs = BigQueryToGCSOperator(
  task_id="bigquery_to_gcs",
  source_project_dataset_table=f"{bq_data_set}.{table}",
  destination_cloud_storage_uris=["gs://gcp-training-bharath/output.csv"],
  dag=dag,
)

finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)

start_pipeline >> load_data_1 >> create_transformation >> [drop_table, archive_file, bq_to_gcs] >> finish_pipeline