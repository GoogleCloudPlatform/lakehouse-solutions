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

-- Snowflake (Finance team): database, schema and Iceberg tables.
-- Run in a Snowflake SQL worksheet. Uses Snowflake-managed Iceberg storage.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS SF_RETAIL_DB;
CREATE SCHEMA IF NOT EXISTS SF_RETAIL_DB.SHOP;

CREATE OR REPLACE ICEBERG TABLE SF_RETAIL_DB.SHOP.CUSTOMERS (
    customer_id  VARCHAR COMMENT 'Customer identifier shared across all teams, for example C0042',
    first_name   VARCHAR,
    last_name    VARCHAR,
    email        VARCHAR,
    country      VARCHAR COMMENT 'ISO country code',
    signup_date  DATE
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
COMMENT = 'Cymbal Shop customer master, owned by Finance';

CREATE OR REPLACE ICEBERG TABLE SF_RETAIL_DB.SHOP.ORDERS (
    order_id      VARCHAR,
    customer_id   VARCHAR,
    order_date    DATE,
    order_amount  NUMBER(10,2) COMMENT 'Order value in USD',
    status        VARCHAR COMMENT 'COMPLETED or RETURNED',
    channel       VARCHAR COMMENT 'WEB, APP or STORE'
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
COMMENT = 'Cymbal Shop orders, owned by Finance';

-- Next: run 02_load_data.sql, then 03_semantic_view.sql.
