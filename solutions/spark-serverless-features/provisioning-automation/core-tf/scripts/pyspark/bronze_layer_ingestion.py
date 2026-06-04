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
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_to_bronze(spark: SparkSession, staging_bucket_name: str, lakehouse_bucket_name: str, data_entity_name: str):
    """
    Loads data from a staging bucket and writes it to the bronze layer in a lakehouse bucket.
    """
    source_path = f"gs://{staging_bucket_name}/froyo-data/{data_entity_name}"
    destination_path = f"gs://{lakehouse_bucket_name}/froyo-raw/bronze/{data_entity_name}"

    logging.info(f"Reading data from: {source_path}")
    # For production workloads, it's a best practice to define an explicit schema
    # rather than using `inferSchema=True`, which can be slow and error-prone.
    df = spark.read.format("parquet").option("inferschema", True).load(source_path)

    logging.info(f"Writing data to: {destination_path}")
    # Write to bronze layer / raw layer
    df.write.mode("overwrite").parquet(destination_path)


def main():
    """Main function to execute the bronze layer ingestion."""
    if len(sys.argv) != 5:
        raise Exception("Exactly 4 arguments are required: <project_id> <staging bucket> <lakehouse bucket> <data_entity_name>")

    project_id = sys.argv[1]
    staging_bucket_name = sys.argv[2]
    lakehouse_bucket_name = sys.argv[3]
    data_entity_name = sys.argv[4]

    spark = None
    try:
        spark = SparkSession.builder \
            .appName(f"Bronze Layer Ingestion for {data_entity_name}") \
            .getOrCreate()

        logging.info(f"Starting bronze layer ingestion of '{data_entity_name}'.")
        ingest_to_bronze(spark, staging_bucket_name, lakehouse_bucket_name, data_entity_name)
        logging.info(f"Successfully completed bronze layer ingestion of '{data_entity_name}'.")

    except Exception as e:
        logging.error(f"An error occurred during Bronze layer ingestion for {data_entity_name}: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
