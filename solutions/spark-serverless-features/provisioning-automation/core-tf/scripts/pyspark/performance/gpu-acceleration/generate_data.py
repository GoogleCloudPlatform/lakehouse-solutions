
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

"""
Data generator for the A/B Test Metric Aggregation benchmark.

Generates 3 types of tables:
  1. activity_log_N (5 tables) - raw user-event metric tables UNION'd in the query
  2. assignment        - user-to-test assignment mapping
  3. test_config       - small test configuration dimension table

Each activity log uses a different metric column name, simulating separate
instrumentation pipelines feeding into a unified metric aggregation job.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)

NUM_ACTIVITY_LOGS = 5
NUM_METRIC_IDS = 200
NUM_TESTS = 30
GROUPS_PER_TEST = 4

MEASURE_COLUMNS = [
    "impressions",
    "interactions",
    "conversions",
    "searches",
    "referrals",
]

METRIC_ID_RANGES = [
    (0, 40),
    (40, 80),
    (80, 120),
    (120, 160),
    (160, 200),
]


def generate_test_config(spark, output_path):
    """
    Small dimension table describing active tests and their metrics.
    After filtering, ~200 rows (one per metric_id).
    """
    print("Generating test_config...")
    rows = []
    for mc in range(NUM_METRIC_IDS):
        tid = mc % NUM_TESTS + 1
        gid = mc % GROUPS_PER_TEST + 1
        cap = 150.0 + (mc % 40) * 12.5
        weight = 0.02 + (mc % 15) * 0.008
        platforms = ["ios", "android", "web", "all"]
        owners = ["growth", "engagement", "monetization", "retention", "infra"]
        stat_methods = ["t_test", "z_test", "mann_whitney", "bayesian", "chi_squared"]
        rows.append((
            f"test_{tid}",
            tid,
            gid,
            "2025-10-01",
            "2025-10-31",
            mc,
            f"domain_{mc % 8}",
            "total",
            f"priority_{mc % 3}",
            cap,
            [f"segment_{mc % 6}"],
            weight,
            "cumulative",
            f"grp_{mc % 8}::{MEASURE_COLUMNS[mc % NUM_ACTIVITY_LOGS]}",
            0.05 if mc % 7 != 0 else 0.01,
            0.8 + (mc % 5) * 0.04,
            "true" if mc % 3 == 0 else "false",
            platforms[mc % len(platforms)],
            owners[mc % len(owners)],
            stat_methods[mc % len(stat_methods)],
            7 + (mc % 4) * 7,
        ))

    schema = StructType([
        StructField("test_name", StringType(), False),
        StructField("test_id", LongType(), False),
        StructField("group_id", LongType(), False),
        StructField("window_start", StringType(), True),
        StructField("window_end", StringType(), True),
        StructField("metric_id", LongType(), False),
        StructField("domain", StringType(), True),
        StructField("agg_method", StringType(), False),
        StructField("priority", StringType(), True),
        StructField("cap_threshold", DoubleType(), False),
        StructField("segment_tags", ArrayType(StringType()), True),
        StructField("bucket_weight", DoubleType(), False),
        StructField("calc_mode", StringType(), True),
        StructField("metric_label", StringType(), True),
        StructField("significance_level", DoubleType(), True),
        StructField("stat_power", DoubleType(), True),
        StructField("is_primary_metric", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("owner_team", StringType(), True),
        StructField("stat_method", StringType(), True),
        StructField("min_exposure_days", LongType(), True),
    ])
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(output_path)
    cnt = spark.read.parquet(output_path).count()
    print(f"  test_config: {cnt} rows -> {output_path}")


def generate_assignment(spark, output_path, num_users):
    """
    User-to-test assignment table.
    Maps each user to a test and group with an assignment timestamp.
    """
    print(f"Generating assignment with {num_users:,} users...")
    df = spark.range(0, num_users, numPartitions=max(10, num_users // 500_000)).select(
        F.concat(F.lit("v_"), F.format_string("%010d", F.col("id"))).alias("visitor_id"),
        (F.col("id") % NUM_TESTS + 1).cast("long").alias("test_id"),
        ((F.col("id") % GROUPS_PER_TEST) + 1).cast("string").alias("group_id"),
        (1735700000 + (F.col("id") % 300000)).cast("long").alias("assigned_at"),
        F.concat(F.lit("test_"), ((F.col("id") % NUM_TESTS) + 1).cast("string")).alias("test_name"),
    )
    df.write.mode("overwrite").parquet(output_path)
    cnt = spark.read.parquet(output_path).count()
    print(f"  assignment: {cnt:,} rows -> {output_path}")


def generate_activity_log(spark, output_path, source_idx, num_rows, num_users):
    """
    Raw user-event metric table. Each source has a unique metric column name
    and covers a range of metric_ids.
    """
    measure_col = MEASURE_COLUMNS[source_idx]
    mid_start, mid_end = METRIC_ID_RANGES[source_idx]
    num_ids = mid_end - mid_start

    num_partitions = max(4, num_rows // 500_000)
    print(f"  Generating activity_log_{source_idx} ({measure_col}): "
          f"{num_rows:,} rows, metric_ids {mid_start}-{mid_end-1}...")

    df = spark.range(0, num_rows, numPartitions=num_partitions).select(
        F.concat(F.lit("v_"), F.format_string("%010d", (F.col("id") % num_users))).alias("visitor_id"),
        (F.rand(seed=17 + source_idx) * 100.0 + 1.0).cast("long").alias(measure_col),
        (1735900000 + (F.col("id") % 86400)).cast("long").alias("event_ts"),
        (F.col("id") % num_ids + mid_start).cast("int").alias("metric_id"),
    )
    df.write.mode("overwrite").parquet(output_path)
    cnt = spark.read.parquet(output_path).count()
    print(f"    -> {cnt:,} rows at {output_path}")


def main():
    parser = argparse.ArgumentParser(description="A/B Test Benchmark Data Generator")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--num-rows", type=int, default=100_000,
                        help="Rows per activity log table (default 100K)")
    parser.add_argument("--num-users", type=int, default=50_000,
                        help="Number of unique visitor IDs (default 50K)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("ABTest_DataGen") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("A/B Test Benchmark - Data Generation")
    print(f"  data_dir:        {args.data_dir}")
    print(f"  rows_per_source: {args.num_rows:,}")
    print(f"  num_users:       {args.num_users:,}")
    print(f"  activity_logs:   {NUM_ACTIVITY_LOGS}")
    print(f"  total_rows:      {args.num_rows * NUM_ACTIVITY_LOGS:,}")
    print("=" * 60)

    generate_test_config(spark, f"{args.data_dir}/test_config")
    generate_assignment(spark, f"{args.data_dir}/assignment", args.num_users)

    print(f"\nGenerating {NUM_ACTIVITY_LOGS} activity log tables...")
    for i in range(NUM_ACTIVITY_LOGS):
        generate_activity_log(
            spark,
            f"{args.data_dir}/activity_log_{i}",
            i,
            args.num_rows,
            args.num_users,
        )

    if args.data_dir.startswith("gs://"):
        print(f"\nData written to GCS: {args.data_dir}")
    else:
        import subprocess
        result = subprocess.run(["du", "-sh", args.data_dir], capture_output=True, text=True)
        print(f"\nTotal data size: {result.stdout.strip()}")
    print("Data generation complete!")
    spark.stop()


if __name__ == "__main__":
    main()
