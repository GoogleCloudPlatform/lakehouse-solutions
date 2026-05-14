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
# This script orchestrates the execution of the cell tower anomany detection jobs
# as a pipeline/workflow with dependencies managed
# ======================================================================================

import os
from airflow.models import Variable
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateBatchOperator,DataprocGetBatchOperator)
from datetime import datetime
from airflow.utils.dates import days_ago
import string
import random 

# Read environment variables into local variables
project_id = models.Variable.get("project_id")
region = models.Variable.get("region")
subnet=models.Variable.get("subnet")
code_bucket=Variable.get("code_bucket")
bq_dataset=Variable.get("bq_dataset")
umsa=Variable.get("umsa")
spark_runtime_version = Variable.get("spark_runtime_version", default_var="2.3")

# Define DAG name
dag_name= "cell_tower_anomaly_detection"

# User Managed Service Account FQN
service_account_id= umsa+"@"+project_id+".iam.gserviceaccount.com"

# PySpark script files in GCS, of the individual Spark applications in the pipeline
curate_customer_script= "gs://"+code_bucket+"/scripts/pyspark/curate_customer_data.py"
curate_telco_performance_metrics_script= "gs://"+code_bucket+"/scripts/pyspark/curate_telco_performance_data.py"
kpis_by_customer_script= "gs://"+code_bucket+"/scripts/pyspark/kpis_by_customer.py"
kpis_by_cell_tower_script= "gs://"+code_bucket+"/scripts/pyspark/kpis_by_cell_tower.py"

# This is to add a random value to the serverless Spark batch ID that needs to be unique each run 
numDigits = 5  # number of digits in the random value.
random_suffix = ''.join(random.choices(string.digits, k = numDigits))

BATCH_ID_PREFIX = "s8s-spark-STUB-"+str(random_suffix)+"-airflow"

CURATE_CUSTOMER_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": curate_customer_script,
        "args": [
          code_bucket
        ]
    },
    "environment_config":{
        "execution_config":{
            "service_account": service_account_id,
            "subnetwork_uri": subnet
        },
        
    },
    "runtime_config": {
        "version": spark_runtime_version
    },
}

CURATE_TELCO_PERFORMANCE_METRICS_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": curate_telco_performance_metrics_script,
        "args": [
          code_bucket
        ]
    },
    "environment_config":{
        "execution_config":{
            "service_account": service_account_id,
            "subnetwork_uri": subnet
        },
        
    },
    "runtime_config": {
        "version": spark_runtime_version
    },
}

CALC_KPIS_BY_CUSTOMER_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": kpis_by_customer_script,
        "args": [
          project_id,
          bq_dataset,
          code_bucket
        ]
    },
    "environment_config":{
        "execution_config":{
            "service_account": service_account_id,
            "subnetwork_uri": subnet
        },
    },
    "runtime_config": {
        "version": spark_runtime_version
    },
}

CALC_KPIS_BY_CELL_TOWER_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": kpis_by_cell_tower_script,
        "args": [
          project_id,
          bq_dataset,
          code_bucket
        ]
    },
    "environment_config":{
        "execution_config":{
            "service_account": service_account_id,
            "subnetwork_uri": subnet
        },
    },
    "runtime_config": {
        "version": spark_runtime_version
    },
}


with models.DAG(
    dag_name,
    schedule_interval=None,
    start_date = days_ago(2),
    catchup=False,
) as dag_serverless_batch:
    curate_customer_master = DataprocCreateBatchOperator(
        task_id="Curate_Customer_Master_Data",
        project_id=project_id,
        region=region,
        batch=CURATE_CUSTOMER_BATCH_CONFIG,
        batch_id= BATCH_ID_PREFIX.replace("STUB", "curate-customer"),
    )
    curate_telco_performance_metrics = DataprocCreateBatchOperator(
        task_id="Curate_Telco_Performance_Metrics",
        project_id=project_id,
        region=region,
        batch=CURATE_TELCO_PERFORMANCE_METRICS_BATCH_CONFIG,
        batch_id=BATCH_ID_PREFIX.replace("STUB", "curate-cell-tower-metrics"),
    )
    calc_kpis_by_customer = DataprocCreateBatchOperator(
        task_id="Calc_KPIs_By_Customer",
        project_id=project_id,
        region=region,
        batch=CALC_KPIS_BY_CUSTOMER_BATCH_CONFIG,
        batch_id=BATCH_ID_PREFIX.replace("STUB", "kpis-by-customer"),
    )
    calc_kpis_by_cell_tower = DataprocCreateBatchOperator(
        task_id="Calc_KPIs_By_Cell_Tower",
        project_id=project_id,
        region=region,
        batch=CALC_KPIS_BY_CELL_TOWER_BATCH_CONFIG,
        batch_id=BATCH_ID_PREFIX.replace("STUB", "kpis-by-cell-tower"),
    )

    curate_customer_master >> curate_telco_performance_metrics
    curate_telco_performance_metrics >> calc_kpis_by_customer
    curate_telco_performance_metrics >> calc_kpis_by_cell_tower
