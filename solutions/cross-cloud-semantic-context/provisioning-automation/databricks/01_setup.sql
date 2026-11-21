-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Databricks (Marketing team): catalog, schema and Iceberg tables.
-- Run in a Databricks SQL workspace. Replace the S3 location with your own
-- external location.

CREATE CATALOG IF NOT EXISTS dbx_retail_db;

CREATE SCHEMA IF NOT EXISTS dbx_retail_db.shop
  MANAGED LOCATION 's3://my-databricks-bucket/dbx_retail_db/shop/';

CREATE TABLE IF NOT EXISTS dbx_retail_db.shop.loyalty_members (
    customer_id     STRING COMMENT 'Customer identifier shared across all teams',
    loyalty_tier    STRING COMMENT 'Gold, Silver or Bronze',
    joined_date     DATE,
    points_balance  INT
)
USING iceberg
COMMENT 'Cymbal Shop loyalty program members, owned by Marketing';

CREATE TABLE IF NOT EXISTS dbx_retail_db.shop.app_visits (
    visit_id         STRING,
    customer_id      STRING,
    visit_date       DATE,
    session_minutes  INT,
    platform         STRING COMMENT 'ios, android or web'
)
USING iceberg
COMMENT 'Cymbal Shop mobile and web app sessions, owned by Marketing';

-- Next: run 02_load_data.sql, then 03_metric_view.sql.
-- Reminder: a metastore admin must enable External data access, as in the prerequisite setup.
