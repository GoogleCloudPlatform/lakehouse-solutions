# LAB: A/B Test Metric Aggregation - Dataproc Serverless Benchmark

## L1: Authenticate

Run the following commands in Cloud Shell to authenticate and configure your active project:

1. Initialization:
`gcloud init`

2. Set the active project target:
`gcloud config set project <YOUR_PROJECT_ID>`

3. Set the quota project for ADC (Application Default Credentials):
`gcloud auth application-default set-quota-project <YOUR_PROJECT_ID>`

<hr>


## L2: Provision the environment

We will use Terraform for infrastructure provisionig in two steps. In the first, we will provision the foundations - API enabling, organization policy related updates, in the second, we will provision service account, IAM permissions, buckets, load data and such. Follow along.

### L2.1. [Terraform] Enable APIs and update organization policy

Run the below in Cloud Shell-
```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`

cd ~/lakehouse-solutions/solutions/performance-benchmarking/provisioning-automation/foundations-tf

terraform init
terraform apply \
  -var="project_id=${PROJECT_ID}" \
  -auto-approve >> spark-serverless-performance-tf-foundations.output
```

You can 
```
tail -f spark-serverless-performance-tf-foundations.output
```



### Core

```
cd ~/lakehouse-solutions-build/solutions/performance-benchmarking/provisioning-automation/core-tf/terraform

PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
PROJECT_NAME=`gcloud projects describe ${PROJECT_ID} | grep name | cut -d':' -f2 | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`
GCP_REGION="us-central1"
DEPLOYER_ACCOUNT_NAME=$GCP_ACCOUNT_NAME
ORG_ID=`gcloud organizations list --format="value(name)"`
S8S_SPARK_RUNTIME_VERSION="2.3"
LAKEHOUSE_RUNTIME_CATALOG_REST_API_VERSION="v1beta"


```

```
terraform init
terraform apply \
  -var="project_id=${PROJECT_ID}" \
  -var="project_name=${PROJECT_NAME}" \
  -var="project_number=${PROJECT_NBR}" \
  -var="gcp_account_name=${GCP_ACCOUNT_NAME}" \
  -var="deployment_service_account_name=${DEPLOYER_ACCOUNT_NAME}" \
  -var="org_id=${ORG_ID}" \
  -var="spark_runtime_version=${S8S_SPARK_RUNTIME_VERSION}" \
  -var="gcp_region=${GCP_REGION}" \
  -var="lrc_rest_api_version=${LAKEHOUSE_RUNTIME_CATALOG_REST_API_VERSION}" \
  -auto-approve >> spark-serverless-performance-tf-core.output
```

Takes ~10 minutes to complete. In a separate cloud shell tab, you can tail the output file for execution state through completion-

```
tail -f ~/lakehouse-solutions/solutions/performance-benchmarking/provisioning-automation/core-tf/terraform/spark-serverless-performance-tf-core.output
```

<hr>




## Overview

This benchmark compares CPU vs GPU (NVIDIA L4) performance and cost for a
production-scale A/B test metric aggregation pipeline on Google Cloud Dataproc
Serverless.

## Data

- **Scale**: 6.5 billion rows (5 activity logs x 1.3B rows each + 200M assignment)
- **Size**: 61.5 GiB on GCS (`gs://haozhu/data/ab_test/`)
- **Tables**: 5 activity logs, 1 assignment table, 1 test config (200 rows)

### Data Generation

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
S8S_SPARK_RUNTIME_VERSION="3.0"
CODE_BUCKET="spark-perf-lab-code-bucket-$PROJECT_NBR"
DATA_BUCKET="spark-perf-lab-data-$PROJECT_NBR"
LOCATION="us-central1"
UMSA_FQN="spark-perf-lab-umsa@$PROJECT_ID.iam.gserviceaccount.com"
SUBNET_NAME="spark-perf-lab-snet"


gcloud dataproc batches submit pyspark \
    "gs://spark-perf-lab-code-bucket-$PROJECT_NBR/scripts/pyspark/gpu_optimization_generate_data.py" \
    --batch "nvidia-abtest-datagen-$(date +%Y%m%d%H%M%S)" \
    --region $LOCATION \
    --version $S8S_SPARK_RUNTIME_VERSION \
    --subnet $SUBNET_NAME \
    --deps-bucket "gs://spark-perf-lab-code-bucket-$PROJECT_NBR" \
    --service-account $UMSA_FQN \
    --properties "\
spark.executor.instances=8,\
spark.executor.cores=8,\
spark.executor.memory=16g,\
spark.driver.cores=4,\
spark.driver.memory=8g,\
spark.dynamicAllocation.enabled=false" \
    -- \
    --data-dir "gs://$DATA_BUCKET/data/ab_test" \
    --num-rows 1300000000 \
    --num-users 200000000
```

~7-8 minutes

## Infrastructure

Both CPU and GPU runs use **identical infrastructure** except for the GPU
accelerator, ensuring a fair apples-to-apples comparison.

| Config | CPU | GPU |
|--------|-----|-----|
| Region | us-central1 | us-central1 |
| Runtime version | 2.3.35 | 2.3.35 |
| Executors | 4 | 4 |
| Executor cores | 8 | 8 |
| Executor memory | 16g | 16g |
| Compute tier | Premium | Premium |
| Disk tier | Premium (SSD 375G) | Premium (SSD 375G) |
| Accelerator | None | **L4 GPU** |
| Dynamic allocation | Disabled | Disabled |
| FileOutputCommitter | v2 | v2 |
| Shuffle partitions | 1000 | 200 |

CPU uses 1000 shuffle partitions to avoid heavy spill on 4 executors, while
GPU handles 200 partitions efficiently due to GPU-accelerated shuffle.

## Benchmark Commands

### CPU run wuth 2 executors
```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
S8S_SPARK_RUNTIME_VERSION="3.0"
CODE_BUCKET="spark-perf-lab-code-bucket-$PROJECT_NBR"
DATA_BUCKET="spark-perf-lab-data-$PROJECT_NBR"
LOCATION="us-central1"
UMSA_FQN="spark-perf-lab-umsa@$PROJECT_ID.iam.gserviceaccount.com"
SUBNET_NAME="spark-perf-lab-snet"

gcloud dataproc batches submit pyspark \
    "gs://spark-perf-lab-code-bucket-$PROJECT_NBR/scripts/pyspark/gpu_optimization_run_benchmark.py" \
    --batch "nvidia-abtest-cpu-2ex-baseline-$(date +%Y%m%d%H%M%S)" \
    --region $LOCATION \
    --version $S8S_SPARK_RUNTIME_VERSION \
    --subnet $SUBNET_NAME \
    --deps-bucket "gs://spark-perf-lab-code-bucket-$PROJECT_NBR" \
    --service-account $UMSA_FQN \
    --properties "\
spark.dynamicAllocation.enabled=false,\
spark.executor.instances=2,\
spark.executor.cores=8,\
spark.executor.memory=16g,\
spark.driver.cores=4,\
spark.driver.memory=8g,\
dataproc.tier=premium, \
spark.dataproc.driver.disk.size=375G,\
spark.dataproc.executor.disk.size=375G,\
spark.sql.autoBroadcastJoinThreshold=-1,\
spark.sql.shuffle.partitions=1000,\
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2,\
spark.eventLog.enabled=true,\
spark.eventLog.dir=gs://$DATA_BUCKET/eventlog" \
    -- \
    --data-dir "gs://$DATA_BUCKET/data/ab_test" \
    --output-dir "gs://$DATA_BUCKET/data/ab_test_output_cpu_2ex" 
```

### CPU run wuth 4 executors

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
S8S_SPARK_RUNTIME_VERSION="3.0"
CODE_BUCKET="spark-perf-lab-code-bucket-$PROJECT_NBR"
DATA_BUCKET="spark-perf-lab-data-$PROJECT_NBR"
LOCATION="us-central1"
UMSA_FQN="spark-perf-lab-umsa@$PROJECT_ID.iam.gserviceaccount.com"
SUBNET_NAME="spark-perf-lab-snet"

gcloud dataproc batches submit pyspark \
    "gs://spark-perf-lab-code-bucket-$PROJECT_NBR/scripts/pyspark/gpu_optimization_run_benchmark.py" \
    --batch "nvidia-abtest-cpu-4ex-baseline-$(date +%Y%m%d%H%M%S)" \
    --region $LOCATION \
    --version $S8S_SPARK_RUNTIME_VERSION \
    --subnet $SUBNET_NAME \
    --deps-bucket "gs://spark-perf-lab-code-bucket-$PROJECT_NBR" \
    --service-account $UMSA_FQN \
    --properties "\
spark.dynamicAllocation.enabled=false,\
spark.executor.instances=4,\
spark.executor.cores=8,\
spark.executor.memory=16g,\
spark.driver.cores=4,\
spark.driver.memory=8g,\
dataproc.tier=premium, \
spark.dataproc.driver.disk.size=375G,\
spark.dataproc.executor.disk.size=375G,\
spark.sql.autoBroadcastJoinThreshold=-1,\
spark.sql.shuffle.partitions=1000,\
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2,\
spark.eventLog.enabled=true,\
spark.eventLog.dir=gs://$DATA_BUCKET/eventlog" \
    -- \
    --data-dir "gs://$DATA_BUCKET/data/ab_test" \
    --output-dir "gs://$DATA_BUCKET/data/ab_test_output_cpu_4ex" 
```

<hr>

### GPU run with 2 executors and Managed Spark runtime version 3.0

```


PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
S8S_SPARK_RUNTIME_VERSION="3.0"
CODE_BUCKET="spark-perf-lab-code-bucket-$PROJECT_NBR"
DATA_BUCKET="spark-perf-lab-data-$PROJECT_NBR"
LOCATION="us-central1"
UMSA_FQN="spark-perf-lab-umsa@$PROJECT_ID.iam.gserviceaccount.com"
SUBNET_NAME="spark-perf-lab-snet"

gcloud dataproc batches submit pyspark \
    "gs://spark-perf-lab-code-bucket-$PROJECT_NBR/scripts/pyspark/gpu_optimization_run_benchmark.py" \
    --batch "nvidia-abtest-gpu-2ex-$(date +%Y%m%d%H%M%S)" \
    --region $LOCATION \
    --version $S8S_SPARK_RUNTIME_VERSION \
    --subnet $SUBNET_NAME \
    --deps-bucket "gs://spark-perf-lab-code-bucket-$PROJECT_NBR" \
    --service-account $UMSA_FQN \
    --properties "\
spark.dynamicAllocation.enabled=false,\
spark.executor.instances=2,\
spark.executor.cores=8,\
spark.executor.memory=16g,\
spark.driver.cores=4,\
spark.driver.memory=8g,\
dataproc.tier=premium, \
spark.dataproc.driver.disk.size=375G,\
spark.dataproc.executor.resource.accelerator.type=l4,\
spark.dataproc.executor.storage.class=performance, \
spark.plugins=com.nvidia.spark.SQLPlugin,\
spark.rapids.sql.enabled=true,\
spark.rapids.memory.pinnedPool.size=4G,\
spark.rapids.sql.concurrentGpuTasks=3,\
spark.task.resource.gpu.amount=0.125,\
spark.sql.autoBroadcastJoinThreshold=-1,\
spark.sql.shuffle.partitions=200,\
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2,\
spark.eventLog.enabled=true,\
spark.eventLog.dir=gs://$DATA_BUCKET/eventlog" \
    -- \
    --data-dir "gs://$DATA_BUCKET/data/ab_test" \
    --output-dir "gs://$DATA_BUCKET/data/ab_test_output_gpu_2ex" 
```

<hr>


### GPU run with 4 executors

```


PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
S8S_SPARK_RUNTIME_VERSION="3.0"
CODE_BUCKET="spark-perf-lab-code-bucket-$PROJECT_NBR"
DATA_BUCKET="spark-perf-lab-data-$PROJECT_NBR"
LOCATION="us-central1"
UMSA_FQN="spark-perf-lab-umsa@$PROJECT_ID.iam.gserviceaccount.com"
SUBNET_NAME="spark-perf-lab-snet"

gcloud dataproc batches submit pyspark \
    "gs://spark-perf-lab-code-bucket-$PROJECT_NBR/scripts/pyspark/gpu_optimization_run_benchmark.py" \
    --batch "nvidia-abtest-gpu-4ex-$(date +%Y%m%d%H%M%S)" \
    --region $LOCATION \
    --version $S8S_SPARK_RUNTIME_VERSION \
    --subnet $SUBNET_NAME \
    --deps-bucket "gs://spark-perf-lab-code-bucket-$PROJECT_NBR" \
    --service-account $UMSA_FQN \
    --properties "\
spark.dynamicAllocation.enabled=false,\
spark.executor.instances=4,\
spark.executor.cores=8,\
spark.executor.memory=16g,\
spark.driver.cores=4,\
spark.driver.memory=8g,\
dataproc.tier=premium, \
spark.dataproc.driver.disk.size=375G,\
spark.dataproc.executor.resource.accelerator.type=l4,\
spark.dataproc.executor.storage.class=performance, \
spark.plugins=com.nvidia.spark.SQLPlugin,\
spark.rapids.sql.enabled=true,\
spark.rapids.memory.pinnedPool.size=4G,\
spark.rapids.sql.concurrentGpuTasks=3,\
spark.task.resource.gpu.amount=0.125,\
spark.sql.autoBroadcastJoinThreshold=-1,\
spark.sql.shuffle.partitions=200,\
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2,\
spark.eventLog.enabled=true,\
spark.eventLog.dir=gs://$DATA_BUCKET/eventlog" \
    -- \
    --data-dir "gs://$DATA_BUCKET/data/ab_test" \
    --output-dir "gs://$DATA_BUCKET/data/ab_test_output_gpu_4ex" 
```


### GPU run with 2 executors and Managed Spark runtime version 2.3
```

PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
S8S_SPARK_RUNTIME_VERSION="2.3"
CODE_BUCKET="spark-perf-lab-code-bucket-$PROJECT_NBR"
DATA_BUCKET="spark-perf-lab-data-$PROJECT_NBR"
LOCATION="us-central1"
UMSA_FQN="spark-perf-lab-umsa@$PROJECT_ID.iam.gserviceaccount.com"
SUBNET_NAME="spark-perf-lab-snet"

gcloud dataproc batches submit pyspark \
    "gs://spark-perf-lab-code-bucket-$PROJECT_NBR/scripts/pyspark/gpu_optimization_run_benchmark.py" \
    --batch "nvidia-abtest-gpu-2ex-2-3-$(date +%Y%m%d%H%M%S)" \
    --region $LOCATION \
    --version $S8S_SPARK_RUNTIME_VERSION \
    --subnet $SUBNET_NAME \
    --deps-bucket "gs://spark-perf-lab-code-bucket-$PROJECT_NBR" \
    --service-account $UMSA_FQN \
    --properties "\
spark.dynamicAllocation.enabled=false,\
spark.executor.instances=2,\
spark.executor.cores=8,\
spark.executor.memory=16g,\
spark.driver.cores=4,\
spark.driver.memory=8g,\
dataproc.tier=premium, \
spark.dataproc.executor.disk.tier=premium, \
spark.dataproc.driver.disk.size=375G,\
spark.dataproc.executor.resource.accelerator.type=l4,\
spark.dataproc.executor.storage.class=performance, \
spark.plugins=com.nvidia.spark.SQLPlugin,\
spark.rapids.sql.enabled=true,\
spark.rapids.memory.pinnedPool.size=4G,\
spark.rapids.sql.concurrentGpuTasks=3,\
spark.task.resource.gpu.amount=0.125,\
spark.sql.autoBroadcastJoinThreshold=-1,\
spark.sql.shuffle.partitions=200,\
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2,\
spark.eventLog.enabled=true,\
spark.eventLog.dir=gs://$DATA_BUCKET/eventlog" \
    -- \
    --data-dir "gs://$DATA_BUCKET/data/ab_test" \
```

## Results

### Performance

| Metric | CPU (4 exec) | CPU (2 exec) | GPU (4 exec) | GPU (2 exec) |
|--------|-------------|-------------|-------------|-------------|
| Query duration | **1,571 s (26.2 min)** | **3,065 s (51.1 min)** | **489 s (8.2 min)** | **938 s (15.6 min)** |
| **Speedup vs CPU (same exec)** | 1.0x | 1.0x | **3.21x** | **3.27x** |
| Input data | 61.5 GiB | 61.5 GiB | 61.5 GiB | 61.5 GiB |
| Executors | 4 | 2 | 4 | 2 |

### Resource Usage

| Metric | CPU (4 exec) | CPU (2 exec) | GPU (4 exec) | GPU (2 exec) |
|--------|-------------|-------------|-------------|-------------|
| milliDcuSeconds | 52,100,100 | 56,364,000 | 17,946,500 | 16,931,250 |
| DCU-hours | 14.47 | 15.66 | 4.98 | 4.70 |
| milliAcceleratorSeconds | N/A | N/A | 2,008,000 | 1,740,000 |
| GPU-hours | N/A | N/A | 0.56 | 0.48 |
| shuffleStorageGbSeconds | 3,370,250 | 3,927,000 | 1,066,750 | 1,080,775 |

### Cost Breakdown

| Cost Component | CPU (4 exec) | CPU (2 exec) | GPU (4 exec) | GPU (2 exec) | Rate |
|----------------|-------------|-------------|-------------|-------------|------|
| DCU (Premium) | $1.29 | $1.39 | $0.44 | $0.42 | $0.089/DCU-hr |
| Accelerator (L4) | — | — | $0.38 | $0.32 | $0.672/GPU-hr |
| Shuffle storage | $0.13 | $0.15 | $0.04 | $0.04 | Premium |
| **TOTAL** | **$1.42** | **$1.54** | **$0.86** | **$0.78** | |
| **Cost vs CPU (same exec)** | 1.0x | 1.0x | **0.61x** | **0.51x** | |

### Summary

| Metric | GPU (4 exec) vs CPU (4 exec) | GPU (2 exec) vs CPU (2 exec) |
|--------|------------------------------|------------------------------|
| Speedup | **3.21x faster** | **3.27x faster** |
| Cost savings | **39% cheaper** ($0.86 vs $1.42) | **49% cheaper** ($0.78 vs $1.54) |
| DCU reduction | 2.91x fewer (4.98 vs 14.47) | 3.33x fewer (4.70 vs 15.66) |
| Time saved | ~18 minutes | ~35 minutes |

## Best CPU vs Best GPU

The best CPU configuration is 4 executors (2 executors is both slower and more
expensive). For GPU, 4 executors gives the best speed while 2 executors gives
the lowest cost.

### Best performance: CPU (4 exec) vs GPU (4 exec)

| Metric | Best CPU | Best GPU | GPU Advantage |
|--------|----------|----------|---------------|
| Duration | 1,571s (26.2 min) | 489s (8.2 min) | **3.21x faster** |
| Total cost | $1.42 | $0.86 | **39% cheaper** |
| DCU-hours | 14.47 | 4.98 | 2.91x fewer |

### Best cost: CPU (4 exec) vs GPU (2 exec)

| Metric | Best CPU | Cheapest GPU | GPU Advantage |
|--------|----------|--------------|---------------|
| Duration | 1,571s (26.2 min) | 938s (15.6 min) | **1.67x faster** |
| Total cost | $1.42 | $0.78 | **45% cheaper** |
| DCU-hours | 14.47 | 4.70 | 3.08x fewer |

## Event Logs

Event logs are saved to `gs://haozhu/eventlog/` for analysis with the Spark
History Server or RAPIDS profiling tools.

## Completed Runs

| Batch ID | Type | Status | Duration | Cost |
|----------|------|--------|----------|------|
| `abtest-datagen-20260411132240` | Data gen (CPU, 8 exec) | SUCCEEDED | ~10 min | — |
| `abtest-cpu-20260411135112` | CPU benchmark (4 exec) | SUCCEEDED | 1571 s | $1.42 |
| `abtest-cpu-2exec-20260413083136` | CPU benchmark (2 exec) | SUCCEEDED | 3065 s | $1.54 |
| `abtest-gpu-20260411135112` | GPU benchmark (4 exec, L4) | SUCCEEDED | 489 s | $0.86 |
| `abtest-gpu-2exec-20260413074958` | GPU benchmark (2 exec, L4) | SUCCEEDED | 938 s | $0.78 |
