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

-- Databricks metric view: Marketing's definitions, including loyalty tiers.
-- Run after 01_setup.sql and 02_load_data.sql.
-- Note: "active_customers" here means app engagement, which differs from Finance's definition.

CREATE OR REPLACE VIEW dbx_retail_db.shop.customer_engagement
WITH METRICS
LANGUAGE YAML
COMMENT 'Marketing engagement metrics for the Cymbal Shop app'
AS $$
version: 1.1
source: dbx_retail_db.shop.app_visits
joins:
  - name: member
    source: dbx_retail_db.shop.loyalty_members
    on: source.customer_id = member.customer_id
dimensions:
  - name: visit_month
    expr: DATE_TRUNC('MONTH', visit_date)
  - name: loyalty_tier
    expr: member.loyalty_tier
    comment: Gold, Silver or Bronze. Null for customers who are not members
  - name: platform
    expr: platform
measures:
  - name: active_customers
    expr: COUNT(DISTINCT CASE WHEN visit_date > DATE_SUB(CURRENT_DATE(), 30) THEN customer_id END)
    display_name: Active customers
    synonyms: ["active users", "active members"]
    comment: Customers who opened the app or website in the last 30 days
  - name: total_visits
    expr: COUNT(visit_id)
  - name: avg_session_minutes
    expr: AVG(session_minutes)
$$;

-- Quick check: total_visits is 817 for the sample data.
-- (active_customers counts the last 30 days, so its value depends on today's date.)
SELECT MEASURE(total_visits) AS total_visits
FROM dbx_retail_db.shop.customer_engagement;
