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

-- Snowflake access for Google Cloud. Two identities:
--   1. The Lakehouse federation identity from the prerequisite setup (query path).
--   2. The Knowledge Catalog connector (metadata and semantic model path).

USE ROLE ACCOUNTADMIN;

-- 1. Let the Lakehouse reader role from the prerequisite setup read the new SHOP tables.
CREATE ROLE IF NOT EXISTS gcp_lakehouse_reader_role;
GRANT USAGE ON DATABASE SF_RETAIL_DB TO ROLE gcp_lakehouse_reader_role;
GRANT USAGE ON SCHEMA SF_RETAIL_DB.SHOP TO ROLE gcp_lakehouse_reader_role;
GRANT SELECT ON TABLE SF_RETAIL_DB.SHOP.CUSTOMERS TO ROLE gcp_lakehouse_reader_role;
GRANT SELECT ON TABLE SF_RETAIL_DB.SHOP.ORDERS TO ROLE gcp_lakehouse_reader_role;

-- 2. Knowledge Catalog connector role and service user, per the Snowflake connector guide:
--    https://docs.cloud.google.com/knowledge-catalog/docs/snowflake-horizon-transfer
--    The connector reads metadata from SNOWFLAKE.ACCOUNT_USAGE with key-pair authentication.
CREATE ROLE IF NOT EXISTS DATAPLEX_CATALOG_READER;
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE DATAPLEX_CATALOG_READER;
GRANT USAGE ON WAREHOUSE <your_warehouse> TO ROLE DATAPLEX_CATALOG_READER;
GRANT USAGE ON DATABASE SF_RETAIL_DB TO ROLE DATAPLEX_CATALOG_READER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SF_RETAIL_DB TO ROLE DATAPLEX_CATALOG_READER;
-- REFERENCES lets the connector see the structure of the tables (including their keys)
-- and of the semantic view, without reading data. All three were granted when we tested.
GRANT REFERENCES ON TABLE SF_RETAIL_DB.SHOP.CUSTOMERS TO ROLE DATAPLEX_CATALOG_READER;
GRANT REFERENCES ON TABLE SF_RETAIL_DB.SHOP.ORDERS TO ROLE DATAPLEX_CATALOG_READER;
GRANT REFERENCES ON SEMANTIC VIEW SF_RETAIL_DB.SHOP.FINANCE_SALES TO ROLE DATAPLEX_CATALOG_READER;

CREATE USER IF NOT EXISTS DATAPLEX_CONNECTOR_USER
  TYPE = SERVICE
  DEFAULT_ROLE = DATAPLEX_CATALOG_READER
  DEFAULT_WAREHOUSE = <your_warehouse>;
GRANT ROLE DATAPLEX_CATALOG_READER TO USER DATAPLEX_CONNECTOR_USER;

-- Key pair for the connector (run locally):
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- ALTER USER DATAPLEX_CONNECTOR_USER SET RSA_PUBLIC_KEY = '<rsa_key.pub without BEGIN/END lines>';

-- Account identifier for the connector form:
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();

-- Note: ACCOUNT_USAGE views lag up to 3 hours (24 for some), so new objects
-- might not appear in the first connector run.
