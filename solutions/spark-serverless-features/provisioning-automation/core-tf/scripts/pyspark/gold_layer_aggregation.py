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
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def aggregate_orders_to_gold(spark: SparkSession):
    """
    Aggregates silver layer order and customer data into a denormalized gold table.
    """
    logging.info("Starting gold layer aggregation for orders.")

    gold_table_sql = """
        SELECT DISTINCT
            oh.order_id,
            oh.order_dt,
            oh.order_total,
            oh.order_item_id,
            oh.product_id,
            oh.product_nm,
            oh.product_recipe,
            oh.unit_price,
            oh.quantity,
            oh.line_total,
            c.customer_id,
            c.customer_nm,
            c.age_bracket AS customer_age_bracket,
            c.income AS customer_income,
            c.city AS customer_city,
            c.state_cd AS customer_state_cd,
            c.zip_cd AS customer_zip_cd,
            c.country_cd AS customer_country_cd,
            c.status AS customer_status,
            c.consent_ts AS customer_consent_ts
        FROM
            froyo_ns.s_order_history oh
        LEFT OUTER JOIN
            froyo_ns.s_customer_master c ON oh.customer_id = c.customer_id
    """

    gold_df = spark.sql(gold_table_sql)

    logging.info("Writing enriched orders data to gold table 'froyo_ns.g_orders_enriched'.")
    gold_df.writeTo("froyo_ns.g_orders_enriched") \
        .partitionedBy(F.months("order_dt")) \
        .tableProperty("write.distribution-mode", "hash") \
        .tableProperty("write.format.default", "parquet") \
        .createOrReplace()


def main():
    """Main function to run the gold layer aggregation process."""
    
    try:
        spark = SparkSession.builder \
            .appName("Gold Layer Aggregation of Orders") \
            .getOrCreate()

        aggregate_orders_to_gold(spark)

        logging.info("Successfully completed gold layer aggregation of orders.")

    except Exception as e:
        logging.error(f"An error occurred during gold layer aggregation of orders: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logging.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    main()
