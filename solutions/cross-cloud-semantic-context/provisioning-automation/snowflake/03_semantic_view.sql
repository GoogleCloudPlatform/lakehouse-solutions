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

-- Snowflake semantic view: Finance's definitions of revenue (net sales)
-- and active customers. Run after 01_setup.sql and 02_load_data.sql.

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE SEMANTIC VIEW SF_RETAIL_DB.SHOP.FINANCE_SALES
  TABLES (
    customers AS SF_RETAIL_DB.SHOP.CUSTOMERS
      PRIMARY KEY (customer_id)
      WITH SYNONYMS = ('shoppers', 'buyers')
      COMMENT = 'One row per Cymbal Shop customer',
    orders AS SF_RETAIL_DB.SHOP.ORDERS
      PRIMARY KEY (order_id)
      COMMENT = 'One row per order. Returned orders keep status RETURNED'
  )
  RELATIONSHIPS (
    orders_to_customers AS orders (customer_id) REFERENCES customers
  )
  FACTS (
    orders.amount_usd AS order_amount
      COMMENT = 'Order value in USD'
  )
  DIMENSIONS (
    customers.customer_country AS country
      COMMENT = 'Customer country code',
    orders.order_day AS order_date
      COMMENT = 'Date the order was placed',
    orders.order_status AS status
      COMMENT = 'COMPLETED or RETURNED',
    orders.sales_channel AS channel
      COMMENT = 'WEB, APP or STORE'
  )
  METRICS (
    orders.net_sales AS SUM(CASE WHEN status = 'COMPLETED' THEN order_amount ELSE 0 END)
      WITH SYNONYMS = ('total sales', 'spend', 'revenue')
      COMMENT = 'Net sales in USD. Completed orders only, returned orders are excluded',
    orders.active_customers AS COUNT(DISTINCT CASE
        WHEN status = 'COMPLETED' AND order_date > DATEADD(day, -90, CURRENT_DATE())
        THEN customer_id END)
      WITH SYNONYMS = ('active buyers', 'active shoppers')
      COMMENT = 'Customers with at least one completed order in the last 90 days'
  )
  COMMENT = 'Finance definitions for Cymbal Shop customers and sales';

-- Quick check: net_sales is 302700.40 for the sample data.
-- (active_customers counts the last 90 days, so it depends on today's date.)
SELECT * FROM SEMANTIC_VIEW(
  SF_RETAIL_DB.SHOP.FINANCE_SALES
  METRICS orders.active_customers, orders.net_sales
);
