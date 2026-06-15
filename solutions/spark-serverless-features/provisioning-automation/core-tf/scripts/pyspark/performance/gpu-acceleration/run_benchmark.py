
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
A/B Test Metric Aggregation benchmark - PySpark job.

Simulates a production-scale A/B testing metric aggregation pipeline:
  1. Read 5 activity log tables (simulates UNION of many instrumentation pipelines)
     Each: Scan -> Filter (non-null, measure >= 1, valid visitor) -> Project
  2. Union all 5 sources
  3. HashAggregate by (visitor_id, metric_id, ts) -> sum(measure_value)
  4. SortMergeJoin INNER with assignment on visitor_id, filter ts within 30-day analysis window
  5. BroadcastHashJoin INNER with test_config on (metric_id, test_id, group_id)
     Config filtered to: agg_method=total, calc_mode!=daily_active, primary metrics, cap not null
  6. HashAggregate by (visitor_id, test_id, group_id, metric_id) -> sum, min(cap), count
  7. Cap measure values at min cap threshold
  8. LeftOuter BroadcastHashJoin with weight lookup (avg weight per metric from config)
  9. Compute bucket_key via xxhash64, repartition, write Parquet partitioned by metric_id
"""

import argparse
import time
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

DATA_DIR = "data"
OUTPUT_DIR = "output"
NUM_SHUFFLE_PARTITIONS = 200

ACTIVITY_LOGS = [
    {"dir": "activity_log_0", "col": "impressions",   "ids": list(range(0, 40))},
    {"dir": "activity_log_1", "col": "interactions",   "ids": list(range(40, 80))},
    {"dir": "activity_log_2", "col": "conversions",    "ids": list(range(80, 120))},
    {"dir": "activity_log_3", "col": "searches",       "ids": list(range(120, 160))},
    {"dir": "activity_log_4", "col": "referrals",      "ids": list(range(160, 200))},
]


def read_and_normalize_source(spark, data_dir, source):
    """
    Read an activity log table and normalize to a common schema:
      (visitor_id: string, measure_value: double, metric_id: int, ts: bigint)
    """
    measure_col = source["col"]
    path = f"{data_dir}/{source['dir']}"

    df = spark.read.parquet(path)

    filtered = df.filter(
        F.col(measure_col).isNotNull() &
        (F.col(measure_col) >= 1) &
        F.col("visitor_id").isNotNull() &
        (F.length("visitor_id") > 0) &
        F.col("event_ts").isNotNull()
    )

    projected = filtered.select(
        F.col("visitor_id"),
        F.col(measure_col).cast("double").alias("measure_value"),
        F.col("metric_id"),
        F.col("event_ts").alias("ts"),
    )
    return projected


def run_query(spark, data_dir, output_dir):
    start = time.time()

    print("Step 1: Reading and normalizing activity logs (Union pattern)...")
    source_dfs = []
    for src in ACTIVITY_LOGS:
        sdf = read_and_normalize_source(spark, data_dir, src)
        source_dfs.append(sdf)
        print(f"  - {src['dir']} ({src['col']}): metric_ids {src['ids'][0]}-{src['ids'][-1]}")

    print("Step 2: Union all activity logs...")
    unioned = reduce(DataFrame.unionAll, source_dfs)

    print("Step 3: Pre-aggregation - group by (visitor_id, metric_id, ts), sum(measure_value)...")
    agg1 = unioned.groupBy("visitor_id", "metric_id", "ts") \
        .agg(F.sum("measure_value").alias("measure_value"))

    print("Step 4: Reading assignment table...")
    assignment = spark.read.parquet(f"{data_dir}/assignment")

    analysis_window_secs = 30 * 86400
    print("Step 5: SortMergeJoin with assignment on visitor_id, filter ts within analysis window...")
    joined = agg1.join(
        assignment,
        (agg1["visitor_id"] == assignment["visitor_id"]) &
        (agg1["ts"] >= assignment["assigned_at"]) &
        (agg1["ts"] < assignment["assigned_at"] + analysis_window_secs),
        "inner"
    ).select(
        agg1["metric_id"],
        agg1["measure_value"],
        assignment["visitor_id"],
        assignment["test_id"],
        assignment["group_id"],
    )

    print("Step 6: Reading test_config...")
    config = spark.read.parquet(f"{data_dir}/test_config")

    filtered_config = config.filter(
        (F.col("agg_method") == "total") &
        (F.col("calc_mode") != "daily_active") &
        (F.col("is_primary_metric") == "true") &
        F.col("cap_threshold").isNotNull()
    )

    print("Step 7: BroadcastHashJoin INNER with config on (metric_id, test_id, group_id)...")
    with_config = joined.join(
        F.broadcast(filtered_config),
        (joined["metric_id"] == filtered_config["metric_id"]) &
        (joined["test_id"] == filtered_config["test_id"]) &
        (joined["group_id"].cast("bigint") == filtered_config["group_id"]),
        "inner"
    ).select(
        joined["visitor_id"],
        joined["test_id"],
        joined["group_id"],
        joined["metric_id"],
        joined["measure_value"],
        filtered_config["cap_threshold"],
    )

    print("Step 8: Final aggregation - group by (visitor_id, test_id, group_id, metric_id)...")
    agg2 = with_config.groupBy("visitor_id", "test_id", "group_id", "metric_id") \
        .agg(
            F.sum("measure_value").alias("total_value"),
            F.min("cap_threshold").alias("min_cap"),
            F.count("*").alias("event_count"),
        )

    capped = agg2.select(
        "visitor_id",
        "test_id",
        "group_id",
        "metric_id",
        F.least(F.col("total_value"), F.col("min_cap")).alias("measure_value"),
        "event_count",
    )

    print("Step 9: LeftOuter BroadcastHashJoin with weight lookup (aggregated config)...")
    weights = filtered_config.groupBy(
        filtered_config["metric_id"].alias("w_metric_id")
    ).agg(
        F.avg("bucket_weight").alias("bucket_weight")
    )

    with_weights = capped.join(
        F.broadcast(weights),
        capped["metric_id"] == weights["w_metric_id"],
        "left_outer"
    )

    print("Step 10: Compute bucket key and write partitioned by metric_id...")
    num_buckets = F.when(
        F.col("bucket_weight").isNull() | F.isnan("bucket_weight"),
        F.lit(75)
    ).otherwise(
        (F.col("bucket_weight") * 200.0 + 25.0).cast("int")
    )
    final = with_weights.select(
        "visitor_id", "test_id", "group_id",
        capped["metric_id"],
        "measure_value",
        "event_count",
        F.abs(F.xxhash64("visitor_id") % num_buckets).alias("bucket_key"),
    )

    result = final.repartition(NUM_SHUFFLE_PARTITIONS, "bucket_key", "metric_id") \
        .select("visitor_id", "test_id", "group_id", "metric_id", "measure_value", "event_count")

    result.write.mode("overwrite").partitionBy("metric_id").parquet(output_dir)

    elapsed = time.time() - start
    print("=" * 60)
    print(f"Query completed in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print("=" * 60)
    return elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=DATA_DIR)
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    parser.add_argument("--shuffle-partitions", type=int, default=NUM_SHUFFLE_PARTITIONS)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("ABTest_MetricAgg") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    print("=" * 60)
    print("A/B Test Metric Aggregation Benchmark")
    print(f"  data_dir:            {args.data_dir}")
    print(f"  output_dir:          {args.output_dir}")
    print(f"  shuffle_partitions:  {args.shuffle_partitions}")
    is_gpu = "rapids" in spark.conf.get("spark.plugins", "").lower()
    print(f"  mode:                {'GPU (RAPIDS)' if is_gpu else 'CPU'}")
    print("=" * 60)

    elapsed = run_query(spark, args.data_dir, args.output_dir)

    print(f"\nFinal result: {elapsed:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
