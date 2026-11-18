#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Builds the Cymbal Shop context set: one document per semantic model, linked to
# the model imported by the Knowledge Catalog connector and to its Lakehouse tables,
# plus one document that describes how customers relate across Snowflake and Databricks.
set -euo pipefail

PROJECT=my-lakehouse-project       # Google Cloud project ID
LOC=us-central1                    # Knowledge Catalog location
CS=cymbal_context                  # Context set ID
DBX_GROUP=databricks_unity         # Entry group used by the Databricks connector
DBX_METASTORE=my_metastore         # Unity Catalog metastore name
SF_GROUP=snowflake_horizon         # Entry group used by the Snowflake connector
SF_ACCOUNT=myorg-myaccount         # Snowflake account identifier

API="https://dataplex.googleapis.com/v1/projects/$PROJECT/locations/$LOC"
EG_PATH="projects/$PROJECT/locations/$LOC/entryGroups"
AUTH=(-H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application/json")

post() {  # post <path> <json body>
  local out
  out=$(curl -sS --fail-with-body -X POST "$API/$1" "${AUTH[@]}" -d "$2") || { echo "Failed: $1 $out"; exit 1; }
  echo "Created: ${1%%\?*} ${1##*=}"
}

doc() {  # doc <entry id> <parent entry id or ""> <document type> <title> <markdown>
  local parent=""
  [ -n "$2" ] && parent="\"parentEntry\": \"$EG_PATH/$CS/entries/$2\","
  post "entryGroups/$CS/entries?entryId=${1//\//%2F}" "{
    \"entryType\": \"projects/dataplex-types/locations/global/entryTypes/document\", $parent
    \"entrySource\": {\"displayName\": \"$4\"},
    \"aspects\": {
      \"dataplex-types.global.document\": {\"data\": {\"type\": \"$3\"}},
      \"dataplex-types.global.overview\": {\"data\": {\"contentType\": \"MARKDOWN\", \"content\": \"$5\"}}}}"
}

link() {  # link <link id> <document entry id> <target entry name>
  post "entryGroups/$CS/entryLinks?entryLinkId=$1" "{
    \"entryLinkType\": \"projects/dataplex-types/locations/global/entryLinkTypes/reference\",
    \"entryReferences\": [{\"name\": \"$EG_PATH/$CS/entries/$2\", \"type\": \"SOURCE\"},
                          {\"name\": \"$3\", \"type\": \"TARGET\"}]}"
}

# 1. The context set
post "entryGroups?entryGroupId=$CS" '{"displayName": "Cymbal Shop context", "type": "context-set"}'
sleep 5

# 2. A root index, folders, one document per semantic model, and a cross-system document
doc agents "" context-set "Cymbal Shop context" \
  "Semantic models for Cymbal Shop: customer_engagement (Databricks) and finance_sales (Snowflake)."
doc semantic-models agents knowledge-base-index "Semantic models" "One document per source semantic model."
doc semantic-models/customer_engagement semantic-models semantic-model "customer_engagement" \
  "Marketing engagement metrics (Databricks). The linked metric view customer_engagement is the source of truth. Look it up for its tables, joins and metrics."
doc semantic-models/finance_sales semantic-models semantic-model "finance_sales" \
  "Finance definitions for customers and sales (Snowflake). The linked semantic view FINANCE_SALES is the source of truth. Look it up for its tables, joins and metrics."

doc documents agents knowledge-base-index "Business rules" "Rules that apply across systems."
doc documents/customers-across-systems documents knowledge-base "Customers across Snowflake and Databricks" \
  "customer_id identifies the same customer in Snowflake (SF_RETAIL_DB.SHOP.CUSTOMERS and ORDERS) and in Databricks (dbx_retail_db.shop.loyalty_members and app_visits). Use it to combine data from the two systems."

# 3. Link each document to its imported semantic model and its Lakehouse tables
LAKE="$EG_PATH/@biglake/entries/biglake.googleapis.com/projects/$PROJECT/catalogs"
CE=semantic-models/customer_engagement
link ce-model   $CE "$EG_PATH/$DBX_GROUP/entries/$DBX_METASTORE/catalogs/dbx_retail_db/namespaces/shop/semantic-models/customer_engagement"
link ce-visits  $CE "$LAKE/dbx_retail_db/namespaces/shop/tables/app_visits"
link ce-members $CE "$LAKE/dbx_retail_db/namespaces/shop/tables/loyalty_members"
FS=semantic-models/finance_sales
link fs-model     $FS "$EG_PATH/$SF_GROUP/entries/$SF_ACCOUNT/catalogs/SF_RETAIL_DB/namespaces/SHOP/semantic-models/FINANCE_SALES"
link fs-orders    $FS "$LAKE/sf_retail_db/namespaces/SHOP/tables/ORDERS"
link fs-customers $FS "$LAKE/sf_retail_db/namespaces/SHOP/tables/CUSTOMERS"
XS=documents/customers-across-systems
link xs-orders    $XS "$LAKE/sf_retail_db/namespaces/SHOP/tables/ORDERS"
link xs-customers $XS "$LAKE/sf_retail_db/namespaces/SHOP/tables/CUSTOMERS"
link xs-members   $XS "$LAKE/dbx_retail_db/namespaces/shop/tables/loyalty_members"
