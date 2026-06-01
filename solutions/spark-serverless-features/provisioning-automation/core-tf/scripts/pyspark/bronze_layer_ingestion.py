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

import sys
from pyspark.sql import SparkSession

if len(sys.argv) != 5:
    raise Exception("Exactly 4 arguments are required: <project_id> <staging bucket> <lakehouse bucket> <data_entity_name> ")


PROJECT_ID = sys.argv[1]
STAGING_BUCKET_NAME = sys.argv[2]
LAKEHOUSE_BUCKET_NAME = sys.argv[3]
DATA_ENTITY_NAME = sys.argv[4]


spark = SparkSession.builder \
    .appName(f"Bronze Layer Ingestion for {DATA_ENTITY_NAME}") \
    .getOrCreate()


try:
    # Load parquet data from GCS staging bucket and persist to bronze (raw) layer with data in full fidelity
    df = spark.read.format("parquet").option("inferschema",True).load(f"gs://{STAGING_BUCKET_NAME}/froyo-data/{DATA_ENTITY_NAME}")

    # Write to bronze layer / raw layer
    df.write.mode("overwrite").parquet(f"gs://{LAKEHOUSE_BUCKET_NAME}/froyo-raw/bronze/{DATA_ENTITY_NAME}")

    print(f"Bronze layer ingestion of {DATA_ENTITY_NAME} complete.")

except Exception as e:
    print(f"An error occurred during Bronze layer ingestion for {DATA_ENTITY_NAME}: {e}")
    raise

finally:
    spark.stop()
