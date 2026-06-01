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

if len(sys.argv) != 4:
    raise Exception("Exactly 3 arguments are required: <project_id> <lakehouse bucket> <data_entity_name>")


PROJECT_ID = sys.argv[1]
LAKEHOUSE_BUCKET_NAME = sys.argv[2]
DATA_ENTITY_NAME = sys.argv[3]


spark = SparkSession.builder \
    .appName(f"Silver Layer Curation for {DATA_ENTITY_NAME}") \
    .getOrCreate()


try:
    # Load bronze layer data in the Lakehouse bucket and curate the same
    spark.read.format("parquet").option("inferschema",True).load(f"gs://{LAKEHOUSE_BUCKET_NAME}/froyo-raw/bronze/{DATA_ENTITY_NAME}").createOrReplaceTempView("b_customer_master")
    spark.read.format("parquet").option("inferschema",True).load(f"gs://{LAKEHOUSE_BUCKET_NAME}/froyo-raw/bronze/regions").createOrReplaceTempView("b_regions")

    if(DATA_ENTITY_NAME == "customers"):
        # Coalescing as we have VERY small data for the purpose of a hands on lab. Consider other Spark optimizations for optimal file sizing without skew.
        silver_df=spark.sql("select c.customer_id, c.customer_nm,get_json_object(c.demographics, '$.age_bracket') AS age_bracket, " \
                            "get_json_object(c.demographics, '$.income') AS income,r.city, r.state_province as state_cd, " \
                            "r.zip_code as zip_cd, r.country as country_cd,c.status,c.consent_ts " \
                            "from b_customer_master c left outer join b_regions r on c.region_id=r.region_id").dropDuplicates()
        silver_df.show(2, truncate=False)
        silver_df.count()
        silver_df.printSchema()
        silver_df.coalesce(1).write.format("iceberg").mode("overwrite").saveAsTable("froyo_ns.s_customer_master")

        print(f"Silver layer curation of {DATA_ENTITY_NAME} complete.")

except Exception as e:
    print(f"An error occurred during silver layer curation for {DATA_ENTITY_NAME}: {e}")
    raise

finally:
    spark.stop()
