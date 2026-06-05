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
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REVENUE_BY_MONTH_SQL = """
       SELECT
        DATE_TRUNC('MONTH', order_dt) AS sales_month,
        SUM(line_total) AS total_revenue
    FROM froyo_ns.g_orders_enriched
    GROUP BY 1
    ORDER BY 1
    """

AVERAGE_ORDER_VALUE_SQL = """SELECT
        DATE_TRUNC('MONTH', order_dt) AS sales_month,
        AVG(order_total) AS average_order_value
    FROM froyo_ns.g_orders_enriched
    GROUP BY 1
    ORDER BY 1
    """

TOP_TEN_PRODUCTS_SQL = """SELECT
        product_nm,
        SUM(line_total) AS total_revenue
    FROM froyo_ns.g_orders_enriched
    GROUP BY product_nm
    ORDER BY total_revenue DESC
    LIMIT 10
"""


CUSTOMER_SEGMENTATION_SQL = """SELECT
        customer_age_bracket,
        COUNT(DISTINCT customer_id) AS number_of_customers,
        SUM(order_total) AS total_spend
    FROM froyo_ns.g_orders_enriched
    GROUP BY customer_age_bracket
    ORDER BY customer_age_bracket
"""


def generate_report(spark: SparkSession, report_sql: str, report_name: str, report_table_name: str):
    """
    Generates reports and materializes them as Iceberg tables
    """
    logging.info("Starting generation of report {}".format(report_name))
    report_df = spark.sql(report_sql)
  

    logging.info(f"Writing the report {report_name} to platinum table {report_table_name}.")
    report_df.writeTo(f"froyo_ns.{report_table_name}") \
        .tableProperty("write.distribution-mode", "hash") \
        .tableProperty("write.format.default", "parquet") \
        .createOrReplace()
    
    logging.info("Completed generation of report {}".format(report_name))



def main():
    """Main function to run the report generation process."""
    if len(sys.argv) != 2:
        raise Exception("Exactly 1 argument is required: <report_name>")

    report_name = sys.argv[1]


    # The SparkSession is expected to be configured with the necessary Iceberg
    # catalog properties at submission time (e.g., via --properties).
    # A mapping from report_name to the corresponding SQL and target table
    report_generation_jobs = {
        "REVENUE_BY_MONTH": (REVENUE_BY_MONTH_SQL, "p_rdm_rev_by_month"),
        "AVERAGE_ORDER_VALUE": (AVERAGE_ORDER_VALUE_SQL, "p_rdm_averge_order_value"),
        "TOP_TEN_PRODUCTS": (TOP_TEN_PRODUCTS_SQL, "p_rdm_top_ten_products_by_revenue"),
        "CUSTOMER_SEGMENTATION": (CUSTOMER_SEGMENTATION_SQL, "p_rdm_customer_segmentation_by_age")
    }

    try:
        spark = SparkSession.builder \
            .appName(f"Report generation of {report_name}") \
            .getOrCreate()

        report_details = report_generation_jobs.get(report_name)
        if report_details:
            report_sql, report_table_name = report_details
            logging.info(f"Starting generation of report '{report_name}'...")
            generate_report(spark, report_sql, report_name, report_table_name)
            logging.info(f"Successfully completed generation of report '{report_name}'.")
        else:
            raise ValueError(f"Unknown report_name: {report_name}")

    except Exception as e:
        logging.error(f"An error occurred during generation of report {report_name}: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logging.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    main()