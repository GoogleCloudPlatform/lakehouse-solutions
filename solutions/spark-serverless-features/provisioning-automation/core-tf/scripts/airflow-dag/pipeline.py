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
num_digits_task_batch_id = 5  # number of digits in the random value.
unique_task_batch_id_suffix = ''.join(random.choices(string.digits, k = num_digits_task_batch_id))

DAG_RUN_ID_PREFIX = ""



def generate_unique_task_batch_id(DAG_RUN_ID_PREFIX):
    return unique_task_batch_id_suffix

# Spark configurations for the serverless Spark batches
spark_properties_with_iceberg_catalog = {
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

spark_properties_foundational = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128mb",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.dataproc.lineage.enabled": "true",
    "spark.openlineage.transport.type": "gcplineage",
    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    "spark.sql.repl.eagerEval.enabled": "True",
    "spark.openlineage.namespace": "froyo_spark_jobs"
}



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
                "properties": spark_properties_foundational
            },
        }
    elif layer == "silver" and data_entity_name in ["customers", "customers_sensitive", "products", "orders"]:
        return {
            "pyspark_batch": {
                "main_python_file_uri": silver_layer_curation_script,
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
                "properties": spark_properties_with_iceberg_catalog
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

silver_data_entities = [
    "customers",
    "customers_sensitive",
    "products",
]

with models.DAG(
    dag_name,
    schedule_interval=None,
    start_date = days_ago(2),
    catchup=False,
) as dag_serverless_batch:

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")
    bronze_to_silver_bridge = EmptyOperator(task_id="bronze_to_silver_bridge")
    silver_to_gold_bridge = EmptyOperator(task_id="silver_to_gold_bridge")

    # Generate the batch config and create tasks for the bronze layer ingestion jobs in a loop, one for each data entity. These tasks will run in parallel.
    bronze_ingestion_parallel_tasks = []
    for data_entity_name in bronze_data_entities:
        task_id = f"ingest_bronze_{data_entity_name}"
        batch_id =  f"af-{{{{ ts_nodash | lower }}}}-{{{{ try_number }}}}-" + generate_unique_task_batch_id()+  f"-bronze-{data_entity_name.replace('_', '-')}"
        batch_config = generate_batch_config("bronze", data_entity_name)

        task = DataprocCreateBatchOperator(
            task_id=task_id,
            project_id=project_id,
            region=region,
            batch=batch_config,
            batch_id=batch_id,
        )
        bronze_ingestion_parallel_tasks.append(task)

    # Generate the batch config and create tasks for the silver layer curation jobs in a loop, one for each data entity. These tasks will run in parallel.
    silver_curation_parallel_tasks = []
    for data_entity_name in silver_data_entities:
        task_id = f"curate_silver_{data_entity_name}"
        batch_id = f"af-{{{{ ts_nodash | lower }}}}-{{{{ try_number }}}}-" + generate_unique_task_batch_id()+ f"-silver-{data_entity_name.replace('_', '-')}"
        batch_config = generate_batch_config("silver", data_entity_name)
    
        task = DataprocCreateBatchOperator(
            task_id=task_id,
            project_id=project_id,
            region=region,
            batch=batch_config,
            batch_id=batch_id,
        )
        silver_curation_parallel_tasks.append(task)

    # This silver curation task for orders is created separately, as the orders curation logic needs to reference the curated products data in the silver layer. Hence, we cannot run the silver curation task for orders in parallel with the other silver curation tasks. We need to run it sequentially after the other silver curation tasks are done, which is what we achieve by setting up the dependencies in the end of this code.
    task_id = f"curate_silver_orders"
    batch_id = f"af-{{{{ ts_nodash | lower }}}}-{{{{ try_number }}}}-" + generate_unique_task_batch_id()+  f"-silver-orders"
    batch_config = generate_batch_config("silver", "orders")

    silver_curation_order_task = DataprocCreateBatchOperator(
        task_id=task_id,
        project_id=project_id,
        region=region,
        batch=batch_config,
        batch_id=batch_id,
    )

    start_task >> bronze_ingestion_parallel_tasks >> bronze_to_silver_bridge >> silver_curation_parallel_tasks >> silver_curation_order_task >> end_task
