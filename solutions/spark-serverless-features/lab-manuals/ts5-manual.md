# A/B Test Metric Aggregation Benchmark

## Use Case

This benchmark reproduces a **production-scale A/B test metric aggregation
pipeline** commonly used by companies running online experiments. The workload
computes per-user metric sums for each active test, enabling data science
teams to measure the causal impact of product changes across hundreds of
metrics simultaneously.

This is a representative workload for any organization operating an
experimentation platform at scale (ad tech, social media, e-commerce,
fintech, etc.).

## Business Logic

### What Is A/B Testing?

A/B testing is how companies decide whether a product change is actually good. Users
are split into two groups randomly:

- **Control (A)** — sees the old version
- **Treatment (B)** — sees the new version

Key metrics (clicks, purchases, engagement, etc.) are then measured to determine
whether the treatment group performs better. This is how streaming platforms evaluate new recommendation algorithms, how
e-commerce sites test checkout flows, or how social media apps test new feed layouts.

### What Does This Pipeline Do?

At scale, a company might be running **hundreds of tests simultaneously**, each
tracking **hundreds of metrics** across **millions of visitors**. This pipeline is the
data engine that powers that analysis. It takes raw user activity (impressions,
interactions, conversions, searches, referrals), filters it to only count events
within a **30-day window after** each visitor was assigned to a test, aggregates the results
per visitor per test, caps outliers so a single power user doesn't skew the results,
and writes the output for downstream statistical analysis (p-values, confidence
intervals, and test decisions).

### Pipeline Steps

The pipeline executes a single, large-scale ETL job that:

1. **Collects raw metric events** from multiple instrumentation pipelines
  (impressions, interactions, conversions, searches, referrals,
   etc.), each producing a separate activity log table.
2. **Normalizes each metric** into a common schema
  `(visitor_id, measure_value, metric_id, timestamp)`. Each source reads a
   specific measure column, filters out null and zero values along with invalid
   visitor IDs, casts to double, and tags with an integer `metric_id`.
3. **Pre-aggregates** the normalized metrics by `(visitor_id, metric_id,
  timestamp)`using`SUM`, collapsing duplicate events from the same visitor and
   metric in the same time window.
4. **Joins with the assignment table** via a SortMergeJoin on `visitor_id`, with
  a windowed inequality filter `ts >= assigned_at AND ts < assigned_at + 30 days`
   (both as bigint epoch seconds). This ensures only events within a 30-day
   analysis window after assignment are counted, preventing both pre-treatment
   bias and stale data contamination.
5. **Joins with test configuration** via BroadcastHashJoin to filter down
  to only the (metric_id, test_id, group_id) combinations in active
   tests. The config is pre-filtered to primary metrics with `agg_method =  "total"`, `calc_mode != "daily_active"`, `is_primary_metric = "true"`,
   and `cap_threshold IS NOT NULL`, and picks up the `cap_threshold` value.
6. **Final aggregation** groups by `(visitor_id, test_id, group_id,
  metric_id)`and computes`SUM(measure_value)`,` MIN(cap_threshold)`,  and` COUNT(*)`(as`event_count`). Measure values are capped using`  LEAST(total, min_cap)` to reduce the influence of outliers on test
   results.
7. **Left-outer joins with a weight lookup** (derived from the same config
  table via `AVG(bucket_weight) GROUP BY metric_id`) to determine the
   average weight per metric, used with `xxhash64` to compute a bucket key
   for balanced output files.
8. **Writes the result** as Parquet, partitioned by `metric_id`, to the
  output path for downstream statistical analysis.

## Table Schemas

### Activity Logs (5 tables, normalized to common schema)


| Column         | Type   | Description                     |
| -------------- | ------ | ------------------------------- |
| visitor_id     | string | Anonymous visitor identifier    |
| {measure_name} | bigint | Source-specific measure count   |
| event_ts       | bigint | Event timestamp (epoch seconds) |
| metric_id      | int    | Metric identifier               |


### Assignment


| Column      | Type   | Description                          |
| ----------- | ------ | ------------------------------------ |
| visitor_id  | string | Visitor identifier                   |
| test_id     | bigint | Test identifier                      |
| group_id    | string | Treatment/control group ID           |
| assigned_at | bigint | Assignment timestamp (epoch seconds) |


### Test Configuration (broadcast, small table)


| Column             | Type        | Description                         |
| ------------------ | ----------- | ----------------------------------- |
| test_name          | string      | Human-readable test name            |
| test_id            | bigint      | Test identifier                     |
| group_id           | bigint      | Group ID                            |
| window_start       | string      | Analysis window start date          |
| window_end         | string      | Analysis window end date            |
| metric_id          | bigint      | Metric identifier                   |
| domain             | string      | Metric domain/category              |
| agg_method         | string      | "total" for this query              |
| priority           | string      | Metric priority level               |
| cap_threshold      | double      | Outlier cap threshold               |
| segment_tags       | arraystring | Segment grouping tags               |
| bucket_weight      | double      | Weight for bucket partitioning      |
| calc_mode          | string      | Calculation mode                    |
| metric_label       | string      | Human-readable metric label         |
| significance_level | double      | p-value threshold (e.g. 0.05, 0.01) |
| stat_power         | double      | Statistical power (e.g. 0.80)       |
| is_primary_metric  | string      | Whether this is a primary metric    |
| platform           | string      | Target platform (ios/android/web)   |
| owner_team         | string      | Team owning this metric             |
| stat_method        | string      | Statistical test method             |
| min_exposure_days  | bigint      | Minimum days before analysis        |


### Output


| Column        | Type   | Partition Key |
| ------------- | ------ | ------------- |
| visitor_id    | string | No            |
| test_id       | bigint | No            |
| group_id      | string | No            |
| metric_id     | int    | **Yes**       |
| measure_value | double | No            |
| event_count   | bigint | No            |


## Benchmark Results (Dataproc Serverless)

**Data scale**: 6.5 billion rows (5 activity logs x 1.3B rows each + 200M
assignment), 61.5 GiB on GCS.

All runs use identical infrastructure (Premium compute/disk, 8 cores and 16 GB
per executor) except executor count and GPU accelerator.

### Performance


| Config | Executors | Duration | Speedup vs CPU (same exec) |
|--------|-----------|----------|----------------------------|
| CPU | 4 | 1,571s (26.2 min) | 1.0x |
| CPU | 2 | 3,065s (51.1 min) | 1.0x |
| **GPU (L4)** | **4** | **489s (8.2 min)** | **3.21x** |
| GPU (L4) | 2 | 938s (15.6 min) | 3.27x |

### Cost

| Config | Executors | DCU Cost | GPU Cost | Total | vs CPU (same exec) |
|--------|-----------|----------|----------|-------|---------------------|
| CPU | 4 | $1.29 | — | **$1.42** | 1.0x |
| CPU | 2 | $1.39 | — | **$1.54** | 1.0x |
| **GPU (L4)** | **4** | **$0.44** | **$0.38** | **$0.86** | **0.61x (39% cheaper)** |
| GPU (L4) | 2 | $0.42 | $0.32 | **$0.78** | **0.51x (49% cheaper)** |

### Best CPU vs Best GPU

| Comparison | Duration | Cost | GPU Advantage |
|------------|----------|------|---------------|
| Best perf: CPU (4 exec) vs GPU (4 exec) | 26.2 min → 8.2 min | $1.42 → $0.86 | **3.21x faster, 39% cheaper** |
| Best cost: CPU (4 exec) vs GPU (2 exec) | 26.2 min → 15.6 min | $1.42 → $0.78 | **1.67x faster, 45% cheaper** |

See `[BENCHMARK.md](BENCHMARK.md)` for full infrastructure details, commands,
and resource usage breakdown.

## Files


| File                   | Description                                   |
| ---------------------- | --------------------------------------------- |
| `generate_data.py`     | PySpark data generator (run via spark-submit) |
| `run_benchmark.py`     | PySpark benchmark query                       |
| `BENCHMARK.md`         | Full benchmark details and commands           |
| `SIMILARITY_REPORT.md` | Comparison with the original production job   |


