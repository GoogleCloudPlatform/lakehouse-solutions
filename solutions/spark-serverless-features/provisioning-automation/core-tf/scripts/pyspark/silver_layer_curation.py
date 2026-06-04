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
from typing import Callable
from pyspark.sql import SparkSession
import pandas as pd
from google.cloud import storage
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Window
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def curate_customers(spark: SparkSession, lakehouse_bucket_name: str, data_entity_name: str):
    # Load bronze layer data in the Lakehouse bucket and curate the same
    spark.read.format("parquet").option("inferschema",True).load(f"gs://{lakehouse_bucket_name}/froyo-raw/bronze/{data_entity_name}").createOrReplaceTempView("b_customer_master")
    spark.read.format("parquet").option("inferschema",True).load(f"gs://{lakehouse_bucket_name}/froyo-raw/bronze/regions").createOrReplaceTempView("b_regions")

    # Flattening the JSON
    # Coalescing as we have VERY small data for the purpose of a hands on lab. 
    # Consider other Spark optimizations for optimal file sizing without skew.
    silver_df=spark.sql("""
        SELECT
            c.customer_id, c.customer_nm,
            get_json_object(c.demographics, '$.age_bracket') AS age_bracket,
            get_json_object(c.demographics, '$.income') AS income,
            r.city, r.state_province as state_cd,
            r.zip_code as zip_cd, r.country as country_cd,
            c.status, c.consent_ts
        FROM b_customer_master c LEFT OUTER JOIN b_regions r ON c.region_id=r.region_id
        """).dropDuplicates()

    silver_df.coalesce(1).write.format("iceberg").mode("overwrite").saveAsTable("froyo_ns.s_customer_master")


def curate_sensitive_customers(spark: SparkSession, lakehouse_bucket_name: str, data_entity_name: str):
    # Load bronze layer data in the Lakehouse bucket and curate the same
    silver_df=spark.read.format("parquet").option("inferschema",True).load(f"gs://{lakehouse_bucket_name}/froyo-raw/bronze/{data_entity_name}").dropDuplicates()

    # Coalescing as we have very small data
    # Consider other Spark optimizations for optimal file sizing without skew.
    silver_df.coalesce(1).write.format("iceberg").mode("overwrite").saveAsTable("froyo_ns.s_customer_master_sensitive")


def curate_products(spark: SparkSession, stage_bucket_name: str, lakehouse_bucket_name: str, data_entity_name: str):
    # Load bronze layer data in the Lakehouse bucket and curate the same
    spark.read.format("parquet").option("inferschema",True).load(f"gs://{lakehouse_bucket_name}/froyo-raw/bronze/{data_entity_name}").dropDuplicates().createOrReplaceTempView("b_product_master")

    # Curate product master data
    silver_df=spark.sql(f"""
        SELECT DISTINCT
            product_id,
            product_name as product_nm,
            unit_price,
            concat("gs://{lakehouse_bucket_name}/froyo-recipe-pdfs/",replace(lower(product_name),' ','_'),'.pdf') as recipe
        FROM b_product_master
        """).dropDuplicates()

    silver_df.coalesce(1).writeTo("froyo_ns.s_product_master") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.target-file-size-bytes", "536870912") \
        .createOrReplace()

    # Copy recipe PDFs
    COMMON_PATH = "froyo-recipe-pdfs"

    # Grabbing only valid product slugs
    df_files_to_copy = spark.sql(f"""
        SELECT DISTINCT
            replace(lower(product_nm), ' ', '_') as slug
        FROM froyo_ns.s_product_master
        WHERE product_nm IS NOT NULL
    """)

    # We will use mapInPandas - it requires a defined return schema
    result_schema = StructType([StructField("status", StringType())])

    # Function for parallel copy
    def parallel_copy_pdfs(pdf_iterator):
        """Vectorized sync using a single client per batch of product names."""
        client = storage.Client()
        source_bucket = client.bucket(stage_bucket_name)
        dest_bucket = client.bucket(lakehouse_bucket_name)

        for pdf in pdf_iterator:
            for slug in pdf['slug']:
                filename = f"{slug}.pdf"
                full_path = f"{COMMON_PATH}/{filename}"

                source_blob = source_bucket.blob(full_path)

                if source_blob.exists():
                    source_bucket.copy_blob(source_blob, dest_bucket, full_path)

            yield pd.DataFrame({'status': ['batch_processed']})

    # High-parallelism across Spark executors for file copy from staging to silver layer
    # We use repartition to ensure we have enough 'worker threads' hitting GCS.
    sync_job = df_files_to_copy.repartition(200).mapInPandas(parallel_copy_pdfs, result_schema)


def curate_orders(spark: SparkSession, lakehouse_bucket_name: str, data_entity_name: str):
    # Load bronze layer data in the Lakehouse bucket and curate the same
    spark.read.format("parquet").option("inferschema",True).load(f"gs://{lakehouse_bucket_name}/froyo-raw/bronze/{data_entity_name}").createOrReplaceTempView("b_orders")
    spark.read.format("parquet").option("inferschema",True).load(f"gs://{lakehouse_bucket_name}/froyo-raw/bronze/order_items").createOrReplaceTempView("b_order_items")

    # 1. Define the window (Grouping by ID, ordering by Date)
    window_spec = Window.partitionBy("order_id").orderBy(F.col("order_date").desc())

    # 2. Filter for only the 'top' record
    deduped_df = spark.table("b_orders") \
        .withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .drop("rank")

    deduped_df=deduped_df.withColumnRenamed("order_date","order_dt").dropDuplicates()
    # Deduped = 299948

    deduped_df.writeTo("froyo_ns.s_orders") \
        .partitionedBy(F.months("order_dt")) \
        .tableProperty("write.distribution-mode", "hash") \
        .tableProperty("write.format.default", "parquet") \
        .createOrReplace()
    
    # Use Broadcast Hint and move logic into SQL for the optimizer
    query = """
    SELECT /*+ BROADCAST(p) */
        DISTINCT
        o.order_id,
        CAST(o.order_dt AS DATE) as order_dt,
        o.order_total,
        o.customer_id,
        oi.order_item_id,
        oi.product_name as product_nm,
        oi.quantity,
        oi.unit_price,
        oi.line_total
    FROM froyo_ns.s_orders o
    JOIN b_order_items oi ON (oi.order_id = o.order_id)
    """

    # Create the DF and drop duplicates once
    silver_df = spark.sql(query).dropDuplicates()

    # Use the "Hash" distribution mode for Iceberg
    # This ensures data is pre-shuffled by the partition key (order_dt)
    # so you don't create a "Small File" disaster.
    silver_df.writeTo("froyo_ns.s_order_history") \
        .partitionedBy(F.months("order_dt")) \
        .tableProperty("write.distribution-mode", "hash") \
        .tableProperty("write.format.default", "parquet") \
        .createOrReplace()
    
    # Add product_id & product_repice columns
    spark.sql("alter table froyo_ns.s_order_history add column product_id long,product_recipe string;")
    query = """MERGE INTO froyo_ns.s_order_history AS oh
            USING froyo_ns.s_product_master AS pm
            ON oh.product_nm = pm.product_nm
            WHEN MATCHED THEN
            UPDATE SET oh.product_id = pm.product_id,
            oh.product_recipe=pm.recipe;"""
    
    # Execute the merge
    spark.sql(query)


def main():
    """Main function to run the silver layer curation process."""
    if len(sys.argv) != 5:
        raise Exception("Exactly 4 arguments are required: <project_id> <stage bucket> <lakehouse bucket> <data_entity_name>")

    project_id = sys.argv[1]
    stage_bucket_name = sys.argv[2]
    lakehouse_bucket_name = sys.argv[3]
    data_entity_name = sys.argv[4]

    # The SparkSession is expected to be configured with the necessary Iceberg
    # catalog properties at submission time (e.g., via --properties).
    spark = SparkSession.builder \
        .appName(f"Silver Layer Curation for {data_entity_name}") \
        .getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS froyo_ns")

    # A mapping from data_entity_name to the corresponding function
    curation_jobs: dict[str, Callable] = {
        "customers": lambda: curate_customers(spark, lakehouse_bucket_name, data_entity_name),
        "customers_sensitive": lambda: curate_sensitive_customers(spark, lakehouse_bucket_name, data_entity_name),
        "products": lambda: curate_products(spark, stage_bucket_name, lakehouse_bucket_name, data_entity_name),
        "orders": lambda: curate_orders(spark, lakehouse_bucket_name, data_entity_name)
    }

    try:
        job_function = curation_jobs.get(data_entity_name)
        if job_function:
            logging.info(f"Starting silver layer curation for '{data_entity_name}'...")
            job_function()
            logging.info(f"Successfully completed silver layer curation for '{data_entity_name}'.")
        else:
            raise ValueError(f"Unknown data_entity_name: {data_entity_name}")

    except Exception as e:
        logging.error(f"An error occurred during silver layer curation for {data_entity_name}: {e}", exc_info=True)
        raise
 

if __name__ == "__main__":
    main()
