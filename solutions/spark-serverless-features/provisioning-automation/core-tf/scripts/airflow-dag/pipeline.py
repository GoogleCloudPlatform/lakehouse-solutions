# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ======================================================================================
# ABOUT
# This script orchestrates the execution of the data engineering pipeline for froyo analytics
# ======================================================================================

import os
from airflow.models import Variable
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateBatchOperator,DataprocGetBatchOperator)
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.utils.dates import days_ago
import string
import random 

# Read environment variables into local variables
project_id = models.Variable.get("project_id")
project_number = models.Variable.get("project_number")
region = models.Variable.get("region")
subnet=models.Variable.get("subnet")
umsa=models.Variable.get("umsa")
spark_runtime_version = models.Variable.get("spark_runtime_version")
lrc_rest_api_version= models.Variable.get("lrc_rest_api_version")

# Other varaiables
dag_name= "froyo_analytics_pipeline"
code_bucket=f"froyo-lab-code-bucket-{project_number}"
staging_bucket_name=f"froyo-lakehouse-staging-{project_number}"
lakehouse_bucket_name= f"froyo_iceberg_lakehouse_catalog_{project_number}"
iceberg_catalog_name = f"froyo_iceberg_lakehouse_catalog_{project_number}"

# User Managed Service Account FQN
service_account_id= umsa+"@"+project_id+".iam.gserviceaccount.com"

# PySpark script files in GCS, of the individual Spark applications in the pipeline
bronze_layer_ingestion_script= "gs://"+code_bucket+"/scripts/pyspark/bronze_layer_ingestion.py"
silver_layer_curation_script= "gs://"+code_bucket+"/scripts/pyspark/silver_layer_curation.py"
gold_layer_aggregation_script= "gs://"+code_bucket+"/scripts/pyspark/gold_layer_aggregation.py"
platinum_layer_reporting_script= "gs://"+code_bucket+"/scripts/pyspark/platinum_layer_reporting.py"

# This is to add a random value to the serverless Spark batch ID that needs to be unique each run 
numDigits = 5  # number of digits in the random value.
random_suffix = ''.join(random.choices(string.digits, k = numDigits))

# Spark configurations for the serverless Spark batches
spark_properties = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128mb",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    f"spark.sql.defaultCatalog": iceberg_catalog_name,
    f"spark.sql.catalog.{iceberg_catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{iceberg_catalog_name}.type": "rest",
    f"spark.sql.catalog.{iceberg_catalog_name}.uri": f"https://biglake.googleapis.com/iceberg/{lrc_rest_api_version}/restcatalog",
    f"spark.sql.catalog.{iceberg_catalog_name}.warehouse": f"gs://{lakehouse_bucket_name}",
    f"spark.sql.catalog.{iceberg_catalog_name}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
    f"spark.sql.catalog.{iceberg_catalog_name}.header.x-goog-user-project": project_id,
    f"spark.sql.catalog.{iceberg_catalog_name}.rest.auth.type": "org.apache.iceberg.gcp.auth.GoogleAuthManager",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    f"spark.sql.catalog.{iceberg_catalog_name}.rest-metrics-reporting-enabled": "false",
    "spark.dataproc.lineage.enabled": "true",
    "spark.openlineage.transport.type": "gcplineage",
    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    "spark.sql.repl.eagerEval.enabled": "True",
    "spark.openlineage.namespace": "froyo_spark_jobs"
}

BATCH_ID_PREFIX = "STUB-"+str(random_suffix)+"-af"

def generate_batch_config(layer: str, data_entity_name: str):
    '''
    This function generates the batch config for a given layer and data entity. It uses the individual script files in GCS as templates and replaces the placeholder values with the actual values for each data entity and layer.
    '''
    if layer == "bronze" and data_entity_name in ["customers", "customers_sensitive", "products", "orders", "order_items", "regions"]:
        return {
            "pyspark_batch": {
                "main_python_file_uri": bronze_layer_ingestion_script,
                "args": [
                  project_id,
                  staging_bucket_name,
                  lakehouse_bucket_name,
                  data_entity_name
                ]
            },
            "environment_config":{
                "execution_config":{
                    "service_account": service_account_id,
                    "subnetwork_uri": subnet
                },
                
            },
            "runtime_config": {
                "version": spark_runtime_version,
                "properties": spark_properties
            },
        }
    
bronze_data_entities = [
    "customers",
    "customers_sensitive",
    "products",
    "orders",
    "order_items",
    "regions"
]

with models.DAG(
    dag_name,
    schedule_interval=None,
    start_date = days_ago(2),
    catchup=False,
) as dag_serverless_batch:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    bronze_ingestion_tasks = []
    for data_entity_name in bronze_data_entities:
        task_id = f"ingest_bronze_{data_entity_name}"
        batch_id = f"{BATCH_ID_PREFIX}-bronze-{data_entity_name.replace('_', '-')}"
        batch_config = generate_batch_config("bronze", data_entity_name)

        task = DataprocCreateBatchOperator(
            task_id=task_id,
            project_id=project_id,
            region=region,
            batch=batch_config,
            batch_id=batch_id,
        )
        bronze_ingestion_tasks.append(task)

    start_task >> bronze_ingestion_tasks >> end_task
