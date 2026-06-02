# Technical Solution 4: Introduction to lakehouse pipeline orchestration with Managed Service for Apache Airflow




# LAB

## L1. Test the Pyspark scripts for bronze layer ingestion

### L1.1. Variables

```
# Google Cloud environment configuration
export PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
export PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
export REGION="us-central1"
export SUBNET_URI="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/spark-froyo-snet"
export SERVICE_ACCOUNT="froyo-lab-umsa@${PROJECT_ID}.iam.gserviceaccount.com"
export SPARK_RUNTIME_VERSION="3.0"

# Bucket and script configuration
export CODE_BUCKET="froyo-lab-code-bucket-$PROJECT_NBR"
export BRONZE_LAYER_PROCESSING_SCRIPT_PATH="gs://${CODE_BUCKET}/scripts/pyspark/bronze_layer_ingestion.py"
export STAGING_BUCKET_NAME="froyo-lakehouse-staging-$PROJECT_NBR"
export LAKEHOUSE_BUCKET_NAME="froyo_iceberg_lakehouse_catalog_$PROJECT_NBR"
```

### L1.2. Test the script for customer data ingestion

```
export DATA_ENTITY_NAME="customers" 

gcloud dataproc batches submit pyspark ${BRONZE_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="bronze-ingestion-${DATA_ENTITY_NAME}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="spark.sql.adaptive.enabled=true,spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb,spark.sql.adaptive.coalescePartitions.enabled=true" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}

```

### L1.3. Test the script for customer sensitive data ingestion

```
export DATA_ENTITY_NAME="customers_sensitive" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`



gcloud dataproc batches submit pyspark ${BRONZE_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="bronze-ingestion-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="spark.sql.adaptive.enabled=true,spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb,spark.sql.adaptive.coalescePartitions.enabled=true" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}
```

### L1.4. Test the script for product data ingestion

```
export DATA_ENTITY_NAME="products" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`

gcloud dataproc batches submit pyspark ${BRONZE_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="bronze-ingestion-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="spark.sql.adaptive.enabled=true,spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb,spark.sql.adaptive.coalescePartitions.enabled=true" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}
```

### L1.5. Test the script for order data ingestion

```
export DATA_ENTITY_NAME="orders" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`


gcloud dataproc batches submit pyspark ${BRONZE_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="bronze-ingestion-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="spark.sql.adaptive.enabled=true,spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb,spark.sql.adaptive.coalescePartitions.enabled=true" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}
```


### L1.6. Test the script for order items data ingestion

```
export DATA_ENTITY_NAME="order_items" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`


gcloud dataproc batches submit pyspark ${BRONZE_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="bronze-ingestion-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="spark.sql.adaptive.enabled=true,spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb,spark.sql.adaptive.coalescePartitions.enabled=true" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}
```

### L1.7. Test the script for regions data ingestion

```
export DATA_ENTITY_NAME="regions" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`


gcloud dataproc batches submit pyspark ${BRONZE_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="bronze-ingestion-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="spark.sql.adaptive.enabled=true,spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb,spark.sql.adaptive.coalescePartitions.enabled=true" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME} 
```

<hr>


## L2. Test the Pyspark scripts for silver layer curation

### L2.1. Variables

```
# Google Cloud environment configuration
export PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
export PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
export REGION="us-central1"
export SUBNET_URI="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/spark-froyo-snet"
export SERVICE_ACCOUNT="froyo-lab-umsa@${PROJECT_ID}.iam.gserviceaccount.com"
export SPARK_RUNTIME_VERSION="3.0"

# Bucket and script configuration
export CODE_BUCKET="froyo-lab-code-bucket-$PROJECT_NBR"
export SILVER_LAYER_PROCESSING_SCRIPT_PATH="gs://${CODE_BUCKET}/scripts/pyspark/silver_layer_curation.py"
export LAKEHOUSE_BUCKET_NAME="froyo_iceberg_lakehouse_catalog_$PROJECT_NBR"
export STAGING_BUCKET_NAME="froyo-lakehouse-staging-$PROJECT_NBR"
export ICEBERG_CATALOG_NAME="froyo_iceberg_lakehouse_catalog_$PROJECT_NBR"
REST_API_VERSION="v1beta"
export SPARK_PROPERTIES="spark.sql.adaptive.enabled=true,spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb,spark.sql.adaptive.coalescePartitions.enabled=true,spark.sql.defaultCatalog=$ICEBERG_CATALOG_NAME,spark.sql.catalog.$ICEBERG_CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog,spark.sql.catalog.$ICEBERG_CATALOG_NAME.type=rest,spark.sql.catalog.$ICEBERG_CATALOG_NAME.uri=https://biglake.googleapis.com/iceberg/$REST_API_VERSION/restcatalog,spark.sql.catalog.$ICEBERG_CATALOG_NAME.warehouse=gs://$LAKEHOUSE_BUCKET_NAME,spark.sql.catalog.$ICEBERG_CATALOG_NAME.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,spark.sql.catalog.$ICEBERG_CATALOG_NAME.header.x-goog-user-project=$PROJECT_ID,spark.sql.catalog.$ICEBERG_CATALOG_NAME.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,
spark.sql.catalog.$ICEBERG_CATALOG_NAME.rest-metrics-reporting-enabled=false,spark.dataproc.lineage.enabled=true,spark.openlineage.transport.type=gcplineage,spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener,spark.sql.repl.eagerEval.enabled=True, spark.openlineage.namespace=froyo_spark_jobs"

```


### L2.1. Curate customer master data

```
export DATA_ENTITY_NAME="customers" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`


gcloud dataproc batches submit pyspark ${SILVER_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="silver-curation-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="${SPARK_PROPERTIES}" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}
```

### L2.2. Curate customer sensitive data

```
export DATA_ENTITY_NAME="customers_sensitive" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`


gcloud dataproc batches submit pyspark ${SILVER_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="silver-curation-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="${SPARK_PROPERTIES}" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}
```

### L2.3. Curate product data

```
export DATA_ENTITY_NAME="products" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`


gcloud dataproc batches submit pyspark ${SILVER_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="silver-curation-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="${SPARK_PROPERTIES}" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}
```

### L2.4. Curate orders data

```
export DATA_ENTITY_NAME="orders" 
BATCH_ID=`echo $DATA_ENTITY_NAME | tr '_' '-'`


gcloud dataproc batches submit pyspark ${SILVER_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="silver-curation-${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="${SPARK_PROPERTIES}" \
    -- \
    ${PROJECT_ID} \
    ${STAGING_BUCKET_NAME} \
    ${LAKEHOUSE_BUCKET_NAME} \
    ${DATA_ENTITY_NAME}
```

<hr>

## L3. Test the Pyspark scripts for gold layer aggregation

### L3.1. Variables

```
# Google Cloud environment configuration
export PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
export PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
export REGION="us-central1"
export SUBNET_URI="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/spark-froyo-snet"
export SERVICE_ACCOUNT="froyo-lab-umsa@${PROJECT_ID}.iam.gserviceaccount.com"
export SPARK_RUNTIME_VERSION="3.0"
REST_API_VERSION="v1beta"

# Bucket and script configuration
export CODE_BUCKET="froyo-lab-code-bucket-$PROJECT_NBR"
export GOLD_LAYER_PROCESSING_SCRIPT_PATH="gs://${CODE_BUCKET}/scripts/pyspark/gold_layer_aggregation.py"
export LAKEHOUSE_BUCKET_NAME="froyo_iceberg_lakehouse_catalog_$PROJECT_NBR"
export ICEBERG_CATALOG_NAME="froyo_iceberg_lakehouse_catalog_$PROJECT_NBR"
export SPARK_PROPERTIES="spark.sql.adaptive.enabled=true,spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb,spark.sql.adaptive.coalescePartitions.enabled=true,spark.sql.defaultCatalog=$ICEBERG_CATALOG_NAME,spark.sql.catalog.$ICEBERG_CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog,spark.sql.catalog.$ICEBERG_CATALOG_NAME.type=rest,spark.sql.catalog.$ICEBERG_CATALOG_NAME.uri=https://biglake.googleapis.com/iceberg/$REST_API_VERSION/restcatalog,spark.sql.catalog.$ICEBERG_CATALOG_NAME.warehouse=gs://$LAKEHOUSE_BUCKET_NAME,spark.sql.catalog.$ICEBERG_CATALOG_NAME.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,spark.sql.catalog.$ICEBERG_CATALOG_NAME.header.x-goog-user-project=$PROJECT_ID,spark.sql.catalog.$ICEBERG_CATALOG_NAME.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,
spark.sql.catalog.$ICEBERG_CATALOG_NAME.rest-metrics-reporting-enabled=false,spark.dataproc.lineage.enabled=true,spark.openlineage.transport.type=gcplineage,spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener,spark.sql.repl.eagerEval.enabled=True, spark.openlineage.namespace=froyo_spark_jobs"

```

### L3.2. Aggregate orders

```
BATCH_ID="gold-layer-aggregation"

gcloud dataproc batches submit pyspark ${GOLD_LAYER_PROCESSING_SCRIPT_PATH} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --batch="${BATCH_ID}-$(date +%s)" \
    --version=${SPARK_RUNTIME_VERSION} \
    --subnet=${SUBNET_URI} \
    --service-account=${SERVICE_ACCOUNT} \
    --properties="${SPARK_PROPERTIES}" 
```

