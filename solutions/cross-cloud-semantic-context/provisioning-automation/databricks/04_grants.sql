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

-- Databricks access for Google Cloud. Two service principals:
--   1. The Lakehouse federation service principal from the prerequisite setup (query path).
--   2. The Knowledge Catalog connector service principal (metadata and semantic model path).
-- Replace the placeholders with your service principals' application IDs.

-- 1. Let the Lakehouse federation service principal read the new shop schema.
GRANT USE CATALOG ON CATALOG dbx_retail_db TO `<lakehouse-sp-application-id>`;
GRANT USE SCHEMA, EXTERNAL USE SCHEMA, SELECT ON SCHEMA dbx_retail_db.shop
  TO `<lakehouse-sp-application-id>`;

-- 2. Knowledge Catalog connector service principal (OAuth client ID and secret).
--    It connects through a SQL warehouse, so also give it the Can use permission
--    on your warehouse. Read-only metadata access, per the connector guide:
--    https://docs.cloud.google.com/knowledge-catalog/docs/databricks-unity-transfer
GRANT USE CATALOG ON CATALOG dbx_retail_db TO `<connector-sp-application-id>`;
GRANT USE SCHEMA, SELECT ON SCHEMA dbx_retail_db.shop TO `<connector-sp-application-id>`;
