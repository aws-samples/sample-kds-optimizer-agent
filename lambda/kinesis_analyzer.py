"""
Kinesis Data Streams usage analyzer.

Collects metrics from CloudWatch and Kinesis APIs to assess stream usage patterns
and recommend the optimal capacity mode.

Designed to handle accounts with hundreds of streams using batch CloudWatch API
calls and concurrent processing.
"""

import math
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Thresholds for mode recommendation
ON_DEMAND_ADVANTAGE_MIN_THROUGHPUT_MIBS = 10  # MiB/s break-even for On-demand Advantage
TRAFFIC_VARIABILITY_THRESHOLD = 3.0  # ratio of p99/avg — above this is "highly variable"
THROTTLE_RATE_THRESHOLD = 0.01  # >1% throttle rate triggers switch away from provisioned
PROVISIONED_COST_SAVING_THRESHOLD = 0.15  # Only recommend provisioned if >15% cheaper than on-demand
SHARD_WRITE_CAPACITY_BYTES = 1_048_576  # 1 MiB/s per shard
EVALUATION_PERIOD_DAYS = 7  # Look back 7 days for metrics

# --- Pricing (us-east-1 baseline, used for relative comparison) ---

# On-demand Standard pricing
ODS_COST_PER_STREAM_HOUR = 0.04
ODS_COST_PER_GB_INGEST = 0.08
ODS_COST_PER_GB_RETRIEVAL = 0.04
ODS_COST_PER_GB_EFO_RETRIEVAL = 0.05
ODS_COST_PER_GB_MONTH_STORAGE_EXTENDED = 0.10  # 24h-7d retention
ODS_COST_PER_GB_MONTH_STORAGE_LONGTERM = 0.023  # >7d retention

# On-demand Advantage pricing
ODA_COST_PER_GB_INGEST = 0.032
ODA_COST_PER_GB_RETRIEVAL = 0.016
ODA_COST_PER_GB_EFO_RETRIEVAL = 0.016
ODA_COST_PER_GB_MONTH_STORAGE = 0.023  # 24h-365d (single rate)
ODA_MIN_COMMITMENT_MIBS = 25  # 25 MiB/s ingest + 25 MiB/s retrieval minimum

# Provisioned pricing
PROV_COST_PER_SHARD_HOUR = 0.015
PROV_COST_PER_MILLION_PUT_UNITS = 0.014
PROV_COST_PER_SHARD_HOUR_EXTENDED_RETENTION = 0.02  # additional for 24h-7d
PROV_COST_PER_GB_MONTH_LONGTERM = 0.023  # >7d storage
PROV_COST_PER_GB_LONGTERM_RETRIEVAL = 0.021  # >7d reads via GetRecords
PROV_COST_PER_GB_EFO_RETRIEVAL = 0.013
PROV_COST_PER_CONSUMER_SHARD_HOUR = 0.015

# Legacy aliases for backward compatibility in tests
ON_DEMAND_COST_PER_GB_INGEST = ODS_COST_PER_GB_INGEST
ON_DEMAND_COST_PER_GB_RETRIEVAL = ODS_COST_PER_GB_RETRIEVAL
PROVISIONED_COST_PER_SHARD_HOUR = PROV_COST_PER_SHARD_HOUR

# Concurrency settings
MAX_WORKERS = 20  # Parallel threads for API calls
BATCH_SIZE = 500  # CloudWatch get_metric_data supports up to 500 queries per call


class KinesisAnalyzer:
    """Analyzes Kinesis Data Streams and recommends capacity modes."""

    def __init__(self, region: str | None = None):
        session = boto3.Session(region_name=region)
        self.kinesis_client = session.client("kinesis")
        self.cloudwatch_client = session.client("cloudwatch")
        self.sts_client = session.client("sts")
        self.region = session.region_name

    def get_account_id(self) -> str:
        return self.sts_client.get_caller_identity()["Account"]

    def list_streams(self) -> list[dict[str, Any]]:
        """List all Kinesis Data Streams with their descriptions."""
        streams = []
        paginator = self.kinesis_client.get_paginator("list_streams")
        for page in paginator.paginate():
            for summary in page.get("StreamSummaries", []):
                streams.append(summary)
        return streams

    def describe_stream(self, stream_name: str) -> dict[str, Any]:
        """Get detailed stream description including mode and shard count."""
        response = self.kinesis_client.describe_stream_summary(StreamName=stream_name)
        return response["StreamDescriptionSummary"]

    def get_stream_metrics(self, stream_name: str) -> dict[str, Any]:
        """Pull CloudWatch metrics for a stream over the evaluation period (single-stream fallback)."""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=EVALUATION_PERIOD_DAYS)
        period = 3600  # 1-hour granularity

        metrics_to_fetch = [
            ("IncomingBytes", "Sum"),
            ("IncomingRecords", "Sum"),
            ("OutgoingBytes", "Sum"),
            ("OutgoingRecords", "Sum"),
            ("WriteProvisionedThroughputExceeded", "Sum"),
            ("ReadProvisionedThroughputExceeded", "Sum"),
        ]

        results = {}
        for metric_name, stat in metrics_to_fetch:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/Kinesis",
                    MetricName=metric_name,
                    Dimensions=[{"Name": "StreamName", "Value": stream_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=[stat, "Average", "Maximum"],
                )
                datapoints = sorted(
                    response.get("Datapoints", []), key=lambda x: x["Timestamp"]
                )
                results[metric_name] = datapoints
            except Exception as e:
                logger.warning(f"Failed to get metric {metric_name} for {stream_name}: {e}")
                results[metric_name] = []

        return results

    def get_metrics_batch(self, stream_names: list[str]) -> dict[str, dict[str, Any]]:
        """
        Fetch metrics for multiple streams using CloudWatch GetMetricData (batch API).

        This is far more efficient than individual GetMetricStatistics calls.
        GetMetricData supports up to 500 metric queries per request.
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=EVALUATION_PERIOD_DAYS)
        period = 3600

        metrics_to_fetch = [
            "IncomingBytes",
            "IncomingRecords",
            "OutgoingBytes",
            "OutgoingRecords",
            "WriteProvisionedThroughputExceeded",
            "ReadProvisionedThroughputExceeded",
        ]

        # Build metric data queries — each stream gets 6 metrics
        all_results = {name: {m: [] for m in metrics_to_fetch} for name in stream_names}

        # Process in batches of ~83 streams (83 * 6 metrics = 498 queries < 500 limit)
        streams_per_batch = BATCH_SIZE // len(metrics_to_fetch)

        for batch_start in range(0, len(stream_names), streams_per_batch):
            batch_streams = stream_names[batch_start:batch_start + streams_per_batch]
            queries = []

            for stream_name in batch_streams:
                safe_id = stream_name.replace("-", "_").replace(".", "_")[:200]
                for metric_name in metrics_to_fetch:
                    query_id = f"{safe_id}_{metric_name}".lower()[:255]
                    query_id = "m_" + "".join(c if c.isalnum() or c == "_" else "_" for c in query_id)
                    queries.append({
                        "Id": query_id[:255],
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/Kinesis",
                                "MetricName": metric_name,
                                "Dimensions": [{"Name": "StreamName", "Value": stream_name}],
                            },
                            "Period": period,
                            "Stat": "Sum",
                        },
                        "ReturnData": True,
                    })

            try:
                paginator = self.cloudwatch_client.get_paginator("get_metric_data")
                for page in paginator.paginate(
                    MetricDataQueries=queries,
                    StartTime=start_time,
                    EndTime=end_time,
                ):
                    for result in page.get("MetricDataResults", []):
                        query_id = result["Id"]
                        for stream_name in batch_streams:
                            safe_id = stream_name.replace("-", "_").replace(".", "_")[:200]
                            for metric_name in metrics_to_fetch:
                                expected_id = f"m_{safe_id}_{metric_name}".lower()
                                expected_id = "".join(c if c.isalnum() or c == "_" else "_" for c in expected_id)[:255]
                                if query_id == expected_id:
                                    timestamps = result.get("Timestamps", [])
                                    values = result.get("Values", [])
                                    datapoints = [
                                        {"Timestamp": ts, "Sum": val}
                                        for ts, val in zip(timestamps, values)
                                    ]
                                    datapoints.sort(key=lambda x: x["Timestamp"])
                                    all_results[stream_name][metric_name] = datapoints
                                    break
                            else:
                                continue
                            break
            except Exception as e:
                logger.error(f"Batch metric fetch failed for batch starting at {batch_start}: {e}")
                for stream_name in batch_streams:
                    try:
                        all_results[stream_name] = self.get_stream_metrics(stream_name)
                    except Exception as inner_e:
                        logger.error(f"Fallback metric fetch failed for {stream_name}: {inner_e}")

        return all_results

    def describe_streams_concurrent(self, stream_names: list[str]) -> dict[str, dict[str, Any]]:
        """Describe multiple streams concurrently."""
        results = {}

        def _describe(name):
            return name, self.describe_stream(name)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_describe, name): name for name in stream_names}
            for future in as_completed(futures):
                stream_name = futures[future]
                try:
                    name, desc = future.result()
                    results[name] = desc
                except Exception as e:
                    logger.error(f"Failed to describe stream {stream_name}: {e}")
                    results[stream_name] = {"error": str(e)}

        return results

    def list_stream_consumers(self, stream_arn: str) -> list[dict[str, Any]]:
        """List registered EFO consumers for a stream."""
        consumers = []
        try:
            paginator = self.kinesis_client.get_paginator("list_stream_consumers")
            for page in paginator.paginate(StreamARN=stream_arn):
                for consumer in page.get("Consumers", []):
                    consumers.append({
                        "ConsumerName": consumer["ConsumerName"],
                        "ConsumerARN": consumer["ConsumerARN"],
                        "ConsumerStatus": consumer["ConsumerStatus"],
                    })
        except Exception as e:
            logger.warning(f"Failed to list consumers for {stream_arn}: {e}")
        return consumers

    def list_consumers_concurrent(self, stream_arns: dict[str, str]) -> dict[str, list[dict[str, Any]]]:
        """
        List EFO consumers for multiple streams concurrently.

        Args:
            stream_arns: dict mapping stream_name -> stream_arn
        Returns:
            dict mapping stream_name -> list of consumer dicts
        """
        results = {}

        def _list(name, arn):
            return name, self.list_stream_consumers(arn)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(_list, name, arn): name
                for name, arn in stream_arns.items()
            }
            for future in as_completed(futures):
                stream_name = futures[future]
                try:
                    name, consumers = future.result()
                    results[name] = consumers
                except Exception as e:
                    logger.error(f"Failed to list consumers for {stream_name}: {e}")
                    results[stream_name] = []

        return results

    def compute_usage_summary(
        self, stream_name: str, metrics: dict[str, Any], stream_desc: dict[str, Any],
        efo_consumer_count: int = 0
    ) -> dict[str, Any]:
        """Compute a usage summary from raw metrics."""
        incoming_bytes_data = metrics.get("IncomingBytes", [])
        outgoing_bytes_data = metrics.get("OutgoingBytes", [])
        write_throttle_data = metrics.get("WriteProvisionedThroughputExceeded", [])
        read_throttle_data = metrics.get("ReadProvisionedThroughputExceeded", [])

        # Calculate throughput stats (bytes per second)
        # Average is computed over the FULL evaluation period (not just hours with data)
        # to prevent short bursts from inflating the sustained throughput picture.
        # Peak/p99 use only actual datapoints since they represent burst capacity.
        total_period_seconds = EVALUATION_PERIOD_DAYS * 24 * 3600

        if incoming_bytes_data:
            total_incoming_bytes = sum(dp.get("Sum", 0) for dp in incoming_bytes_data)
            avg_incoming_bps = total_incoming_bytes / total_period_seconds
            incoming_rates = [dp["Sum"] / 3600 for dp in incoming_bytes_data if "Sum" in dp]
            max_incoming_bps = max(incoming_rates) if incoming_rates else 0
            p99_incoming_bps = (
                sorted(incoming_rates)[int(len(incoming_rates) * 0.99)]
                if len(incoming_rates) > 1
                else max_incoming_bps
            )
        else:
            avg_incoming_bps = max_incoming_bps = p99_incoming_bps = 0

        if outgoing_bytes_data:
            total_outgoing_bytes = sum(dp.get("Sum", 0) for dp in outgoing_bytes_data)
            avg_outgoing_bps = total_outgoing_bytes / total_period_seconds
            outgoing_rates = [dp["Sum"] / 3600 for dp in outgoing_bytes_data if "Sum" in dp]
            max_outgoing_bps = max(outgoing_rates) if outgoing_rates else 0
        else:
            avg_outgoing_bps = max_outgoing_bps = 0

        # Calculate throttle rate
        total_write_throttles = sum(dp.get("Sum", 0) for dp in write_throttle_data)
        total_read_throttles = sum(dp.get("Sum", 0) for dp in read_throttle_data)
        incoming_records_data = metrics.get("IncomingRecords", [])
        total_records = sum(dp.get("Sum", 0) for dp in incoming_records_data)
        throttle_rate = (
            (total_write_throttles + total_read_throttles) / max(total_records, 1)
        )

        # Traffic variability — ratio of peak to average
        variability_ratio = (
            p99_incoming_bps / avg_incoming_bps if avg_incoming_bps > 0 else 0
        )

        # Cost comparison: on-demand vs provisioned
        open_shard_count = stream_desc.get("OpenShardCount", 0)
        retention_hours = stream_desc.get("RetentionPeriodHours", 24)
        hours_in_period = EVALUATION_PERIOD_DAYS * 24

        # Total volumes over the evaluation period
        total_ingest_gb = sum(dp.get("Sum", 0) for dp in incoming_bytes_data) / (1024**3)
        total_retrieval_gb = sum(dp.get("Sum", 0) for dp in outgoing_bytes_data) / (1024**3)

        # Estimate EFO retrieval: each EFO consumer gets a full copy of ingested data
        efo_retrieval_gb = total_ingest_gb * efo_consumer_count
        # Standard retrieval is the remainder (OutgoingBytes includes both)
        standard_retrieval_gb = max(0, total_retrieval_gb - efo_retrieval_gb)

        # --- On-demand Standard cost ---
        ods_ingest_cost = total_ingest_gb * ODS_COST_PER_GB_INGEST
        ods_retrieval_cost = standard_retrieval_gb * ODS_COST_PER_GB_RETRIEVAL
        ods_efo_retrieval_cost = efo_retrieval_gb * ODS_COST_PER_GB_EFO_RETRIEVAL
        ods_stream_cost = 1 * ODS_COST_PER_STREAM_HOUR * hours_in_period  # 1 stream
        # Retention storage cost for On-demand Standard
        daily_ingest_gb = total_ingest_gb / EVALUATION_PERIOD_DAYS
        ods_storage_cost = 0.0
        if retention_hours > 24:
            retention_days = retention_hours / 24
            # Extended retention (24h to 7d)
            extended_days = min(retention_days, 7) - 1  # days beyond first 24h
            if extended_days > 0:
                # GB stored = daily ingest × days of data kept in extended window
                extended_storage_gb = daily_ingest_gb * extended_days
                # Convert to GB-month (divide by 30 for proportional month)
                ods_storage_cost += extended_storage_gb * ODS_COST_PER_GB_MONTH_STORAGE_EXTENDED * (EVALUATION_PERIOD_DAYS / 30)
            # Long-term retention (>7d)
            if retention_days > 7:
                longterm_days = retention_days - 7
                longterm_storage_gb = daily_ingest_gb * longterm_days
                ods_storage_cost += longterm_storage_gb * ODS_COST_PER_GB_MONTH_STORAGE_LONGTERM * (EVALUATION_PERIOD_DAYS / 30)
        ods_total_cost = ods_ingest_cost + ods_retrieval_cost + ods_efo_retrieval_cost + ods_stream_cost + ods_storage_cost

        # --- On-demand Advantage cost ---
        oda_ingest_cost = total_ingest_gb * ODA_COST_PER_GB_INGEST
        oda_retrieval_cost = standard_retrieval_gb * ODA_COST_PER_GB_RETRIEVAL
        oda_efo_retrieval_cost = efo_retrieval_gb * ODA_COST_PER_GB_EFO_RETRIEVAL
        # No per-stream charge for Advantage
        oda_storage_cost = 0.0
        if retention_hours > 24:
            retention_days = retention_hours / 24
            storage_days = retention_days - 1  # days beyond first 24h (single rate)
            if storage_days > 0:
                oda_storage_gb = daily_ingest_gb * storage_days
                oda_storage_cost = oda_storage_gb * ODA_COST_PER_GB_MONTH_STORAGE * (EVALUATION_PERIOD_DAYS / 30)
        oda_total_cost = oda_ingest_cost + oda_retrieval_cost + oda_efo_retrieval_cost + oda_storage_cost

        # --- Provisioned cost ---
        prov_shard_cost = open_shard_count * PROV_COST_PER_SHARD_HOUR * hours_in_period
        # PUT Payload Units: ceil(avg_record_size / 25KB) * total_records
        avg_record_bytes = (total_ingest_gb * (1024**3)) / max(total_records, 1) if total_records > 0 else 0
        avg_record_kb = avg_record_bytes / 1024
        put_units_per_record = math.ceil(avg_record_kb / 25) if avg_record_kb > 0 else 1
        total_put_units = total_records * put_units_per_record
        prov_put_cost = (total_put_units / 1_000_000) * PROV_COST_PER_MILLION_PUT_UNITS
        # EFO costs for provisioned
        prov_efo_consumer_shard_cost = efo_consumer_count * open_shard_count * PROV_COST_PER_CONSUMER_SHARD_HOUR * hours_in_period
        prov_efo_retrieval_cost = efo_retrieval_gb * PROV_COST_PER_GB_EFO_RETRIEVAL
        # Retention cost for provisioned
        prov_retention_cost = 0.0
        if retention_hours > 24:
            retention_days = retention_hours / 24
            # Extended retention (24h-7d): per shard-hour surcharge
            if retention_days <= 7:
                prov_retention_cost = open_shard_count * PROV_COST_PER_SHARD_HOUR_EXTENDED_RETENTION * hours_in_period
            else:
                # First 7 days: shard-hour surcharge
                prov_retention_cost = open_shard_count * PROV_COST_PER_SHARD_HOUR_EXTENDED_RETENTION * hours_in_period
                # Beyond 7 days: GB-month storage
                longterm_days = retention_days - 7
                longterm_storage_gb = daily_ingest_gb * longterm_days
                prov_retention_cost += longterm_storage_gb * PROV_COST_PER_GB_MONTH_LONGTERM * (EVALUATION_PERIOD_DAYS / 30)
        prov_total_cost = prov_shard_cost + prov_put_cost + prov_efo_consumer_shard_cost + prov_efo_retrieval_cost + prov_retention_cost

        # Cost saving ratios
        if ods_total_cost > 0:
            provisioned_saving_ratio = (ods_total_cost - prov_total_cost) / ods_total_cost
        else:
            provisioned_saving_ratio = 0.0

        return {
            "stream_name": stream_name,
            "evaluation_period_days": EVALUATION_PERIOD_DAYS,
            "avg_incoming_bytes_per_sec": round(avg_incoming_bps, 2),
            "max_incoming_bytes_per_sec": round(max_incoming_bps, 2),
            "p99_incoming_bytes_per_sec": round(p99_incoming_bps, 2),
            "avg_outgoing_bytes_per_sec": round(avg_outgoing_bps, 2),
            "max_outgoing_bytes_per_sec": round(max_outgoing_bps, 2),
            "avg_incoming_mibs": round(avg_incoming_bps / (1024 * 1024), 4),
            "max_incoming_mibs": round(max_incoming_bps / (1024 * 1024), 4),
            "avg_outgoing_mibs": round(avg_outgoing_bps / (1024 * 1024), 4),
            "traffic_variability_ratio": round(variability_ratio, 2),
            "write_throttle_rate": round(throttle_rate, 6),
            "total_write_throttles": int(total_write_throttles),
            "total_read_throttles": int(total_read_throttles),
            "open_shard_count": open_shard_count,
            "retention_hours": retention_hours,
            "efo_consumer_count": efo_consumer_count,
            "total_ingest_gb": round(total_ingest_gb, 4),
            "total_retrieval_gb": round(total_retrieval_gb, 4),
            "efo_retrieval_gb": round(efo_retrieval_gb, 4),
            "standard_retrieval_gb": round(standard_retrieval_gb, 4),
            "total_records": int(total_records),
            "avg_record_bytes": round(avg_record_bytes, 2),
            # Three-way cost estimates (over evaluation period)
            "ods_cost_estimate": round(ods_total_cost, 4),
            "oda_cost_estimate": round(oda_total_cost, 4),
            "provisioned_cost_estimate": round(prov_total_cost, 4),
            # Legacy fields for backward compatibility
            "on_demand_cost_estimate": round(ods_total_cost, 4),
            "provisioned_saving_ratio": round(provisioned_saving_ratio, 4),
        }

    def recommend_mode(
        self, usage_summary: dict[str, Any], current_mode: str
    ) -> dict[str, str]:
        """
        Recommend a per-stream capacity mode based on usage patterns.

        This method only recommends PROVISIONED or ON_DEMAND per stream.
        On-demand Advantage is an account-level decision handled separately
        by _assess_advantage_eligibility().

        Decision hierarchy (strongly biased toward on-demand):
        1. If provisioned and experiencing throttling -> on-demand
        2. If traffic is highly variable/unpredictable -> on-demand
        3. If provisioned is >15% cheaper AND traffic is stable -> stay provisioned
        4. Default -> on-demand (less ops, auto-scaling, no capacity planning)
        """
        variability = usage_summary["traffic_variability_ratio"]
        throttle_rate = usage_summary["write_throttle_rate"]
        provisioned_saving = usage_summary["provisioned_saving_ratio"]

        # Rule 1: Throttling on provisioned streams -> move to on-demand
        if current_mode == "PROVISIONED" and throttle_rate > THROTTLE_RATE_THRESHOLD:
            return {
                "recommendation": "ON_DEMAND",
                "reasoning": (
                    f"Stream is experiencing throttling (rate: {throttle_rate:.4%}) in provisioned mode. "
                    f"Total write throttles: {usage_summary['total_write_throttles']}, "
                    f"read throttles: {usage_summary['total_read_throttles']}. "
                    "On-demand mode eliminates throttling with automatic scaling and no manual shard management."
                ),
                "priority": "HIGH",
            }

        # Rule 2: Highly variable traffic -> On-demand
        if variability > TRAFFIC_VARIABILITY_THRESHOLD:
            return {
                "recommendation": "ON_DEMAND",
                "reasoning": (
                    f"Traffic variability ratio is {variability:.1f}x (p99/avg), indicating highly "
                    f"unpredictable patterns. On-demand mode auto-scales without capacity planning. "
                    f"Max throughput: {usage_summary['max_incoming_mibs']:.2f} MiB/s, "
                    f"Avg: {usage_summary['avg_incoming_mibs']:.2f} MiB/s."
                ),
                "priority": "MEDIUM",
            }

        # Rule 3: Only stay provisioned if cost saving is >15% AND traffic is stable
        if (
            current_mode == "PROVISIONED"
            and provisioned_saving > PROVISIONED_COST_SAVING_THRESHOLD
            and variability <= TRAFFIC_VARIABILITY_THRESHOLD
            and throttle_rate <= THROTTLE_RATE_THRESHOLD
        ):
            ods_cost = usage_summary.get("ods_cost_estimate", usage_summary.get("on_demand_cost_estimate", 0))
            prov_cost = usage_summary.get("provisioned_cost_estimate", 0)
            return {
                "recommendation": "PROVISIONED",
                "reasoning": (
                    f"Provisioned mode is {provisioned_saving:.0%} cheaper than On-demand for this "
                    f"stream's current traffic pattern (stable, variability: {variability:.1f}x). "
                    f"Estimated weekly cost — Provisioned: ${prov_cost:.2f}, "
                    f"On-demand: ${ods_cost:.2f}. "
                    f"However, consider switching to on-demand for operational simplicity if the cost "
                    f"difference is acceptable."
                ),
                "priority": "LOW",
            }

        # Rule 4: Default -> On-demand
        return {
            "recommendation": "ON_DEMAND",
            "reasoning": (
                f"On-demand is recommended for automatic scaling and zero operational overhead. "
                f"Avg throughput: {usage_summary['avg_incoming_mibs']:.2f} MiB/s, variability: {variability:.1f}x. "
                f"No capacity planning needed, scales automatically with traffic."
            ),
            "priority": "LOW",
        }

    def analyze_all_streams(self) -> dict[str, Any]:
        """
        Run full analysis across all streams in the region.

        Uses batch CloudWatch API and concurrent DescribeStream calls
        to handle accounts with hundreds of streams within Lambda timeout.
        """
        account_id = self.get_account_id()
        streams = self.list_streams()

        if not streams:
            return {
                "region": self.region,
                "account_id": account_id,
                "streams": [],
                "account_level_summary": {
                    "total_streams": 0,
                    "message": "No Kinesis Data Streams found in this region.",
                },
            }

        logger.info(f"Analyzing {len(streams)} streams in {self.region}")

        stream_names = [s["StreamName"] for s in streams]
        stream_modes = {
            s["StreamName"]: s.get("StreamModeDetails", {}).get("StreamMode", "PROVISIONED")
            for s in streams
        }

        # Step 1: Describe all streams concurrently (for shard counts, ARNs, retention)
        logger.info("Describing streams concurrently...")
        descriptions = self.describe_streams_concurrent(stream_names)

        # Step 2: List EFO consumers for all streams concurrently
        logger.info("Listing EFO consumers concurrently...")
        stream_arns = {
            name: desc.get("StreamARN", "")
            for name, desc in descriptions.items()
            if "error" not in desc and desc.get("StreamARN")
        }
        all_consumers = self.list_consumers_concurrent(stream_arns)

        # Step 3: Fetch metrics in batch using GetMetricData
        logger.info("Fetching metrics in batch...")
        all_metrics = self.get_metrics_batch(stream_names)

        # Step 4: Compute summaries
        logger.info("Computing usage summaries...")
        stream_results = []
        total_on_demand_incoming_mibs = 0.0
        total_all_incoming_mibs = 0.0  # All streams (for Advantage assessment)
        total_on_demand_streams = sum(1 for m in stream_modes.values() if m != "PROVISIONED")
        total_efo_consumers_in_account = sum(len(c) for c in all_consumers.values())

        for stream_name in stream_names:
            stream_mode = stream_modes[stream_name]
            desc = descriptions.get(stream_name, {})

            if "error" in desc:
                stream_results.append({
                    "stream_name": stream_name,
                    "current_mode": stream_mode,
                    "error": desc["error"],
                })
                continue

            metrics = all_metrics.get(stream_name, {})
            consumers = all_consumers.get(stream_name, [])
            efo_consumer_count = len(consumers)

            try:
                usage_summary = self.compute_usage_summary(
                    stream_name, metrics, desc, efo_consumer_count=efo_consumer_count
                )

                # Track throughput for all streams (Advantage is account-level)
                total_all_incoming_mibs += usage_summary["avg_incoming_mibs"]

                if stream_mode != "PROVISIONED":
                    total_on_demand_incoming_mibs += usage_summary["avg_incoming_mibs"]

                stream_results.append({
                    "stream_name": stream_name,
                    "current_mode": stream_mode,
                    "current_shards": desc.get("OpenShardCount", 0),
                    "stream_arn": desc.get("StreamARN", ""),
                    "retention_hours": desc.get("RetentionPeriodHours", 24),
                    "efo_consumers": consumers,
                    "efo_consumer_count": efo_consumer_count,
                    "metrics_summary": usage_summary,
                })
            except Exception as e:
                logger.error(f"Error computing summary for {stream_name}: {e}")
                stream_results.append({
                    "stream_name": stream_name,
                    "current_mode": stream_mode,
                    "error": str(e),
                })

        # Step 5: Generate recommendations
        logger.info("Generating recommendations...")
        total_on_demand_outgoing_mibs = sum(
            r["metrics_summary"]["avg_outgoing_mibs"]
            for r in stream_results
            if "metrics_summary" in r and r["current_mode"] != "PROVISIONED"
        )
        total_all_outgoing_mibs = sum(
            r["metrics_summary"]["avg_outgoing_mibs"]
            for r in stream_results
            if "metrics_summary" in r
        )
        # Use ALL streams' throughput for Advantage eligibility (account-level decision)
        # since provisioned streams being recommended to switch would contribute once moved
        aggregate_throughput = total_all_incoming_mibs + total_all_outgoing_mibs
        # Count all streams (provisioned ones will be recommended to switch to on-demand)
        total_streams_count = len(streams)

        for result in stream_results:
            if "error" in result:
                result["recommendation"] = "UNABLE_TO_ASSESS"
                result["reasoning"] = f"Analysis failed: {result['error']}"
                continue

            rec = self.recommend_mode(
                result["metrics_summary"],
                result["current_mode"],
            )
            result["recommendation"] = rec["recommendation"]
            result["reasoning"] = rec["reasoning"]
            result["priority"] = rec["priority"]

        # Step 6: Account-level Advantage assessment
        advantage_assessment = self._assess_advantage_eligibility(
            aggregate_throughput, total_streams_count,
            total_efo_consumers_in_account, stream_results
        )

        logger.info(f"Analysis complete. {len(stream_results)} streams processed.")

        return {
            "region": self.region,
            "account_id": account_id,
            "streams": stream_results,
            "account_level_summary": {
                "total_streams": len(streams),
                "provisioned_streams": sum(
                    1 for r in stream_results if r.get("current_mode") == "PROVISIONED"
                ),
                "on_demand_streams": total_on_demand_streams,
                "aggregate_on_demand_ingest_mibs": round(total_on_demand_incoming_mibs, 4),
                "aggregate_on_demand_retrieval_mibs": round(total_on_demand_outgoing_mibs, 4),
                "aggregate_all_ingest_mibs": round(total_all_incoming_mibs, 4),
                "aggregate_all_retrieval_mibs": round(total_all_outgoing_mibs, 4),
                "total_efo_consumers": total_efo_consumers_in_account,
                "on_demand_advantage_eligible": aggregate_throughput >= ON_DEMAND_ADVANTAGE_MIN_THROUGHPUT_MIBS,
                "advantage_assessment": advantage_assessment,
            },
        }

    def _assess_advantage_eligibility(
        self, aggregate_throughput_mibs: float, total_streams: int,
        total_efo_consumers: int, stream_results: list[dict]
    ) -> dict[str, Any]:
        """
        Produce an account-level On-demand Advantage assessment.

        On-demand Advantage is an account-level setting per region.
        This assessment is separate from per-stream mode recommendations.
        """
        triggers = []
        if aggregate_throughput_mibs >= ON_DEMAND_ADVANTAGE_MIN_THROUGHPUT_MIBS:
            triggers.append(f"Aggregate throughput {aggregate_throughput_mibs:.2f} MiB/s >= 10 MiB/s")
        max_efo_per_stream = max(
            (r.get("efo_consumer_count", 0) for r in stream_results if "error" not in r),
            default=0,
        )
        if max_efo_per_stream > 2:
            triggers.append(f"A stream has {max_efo_per_stream} EFO consumers (>2)")
        if total_streams > 50:
            triggers.append(f"{total_streams} streams in account (more than 50)")

        eligible = len(triggers) > 0

        # Estimate savings: compare ODS vs ODA cost for ALL streams
        # (since Advantage applies account-wide to all on-demand streams,
        # and we're recommending provisioned streams switch to on-demand)
        total_ods_cost = sum(
            r["metrics_summary"]["ods_cost_estimate"]
            for r in stream_results
            if "metrics_summary" in r
        )
        total_oda_cost = sum(
            r["metrics_summary"]["oda_cost_estimate"]
            for r in stream_results
            if "metrics_summary" in r
        )

        # Check minimum commitment shortfall
        # 25 MiB/s = 25 * 1024 * 1024 bytes/sec → GB over evaluation period
        min_commitment_gb_ingest = ODA_MIN_COMMITMENT_MIBS * (1024 * 1024) * (EVALUATION_PERIOD_DAYS * 24 * 3600) / (1024**3)
        min_commitment_gb_retrieval = min_commitment_gb_ingest  # same 25 MiB/s
        min_commitment_cost = (min_commitment_gb_ingest * ODA_COST_PER_GB_INGEST) + (min_commitment_gb_retrieval * ODA_COST_PER_GB_RETRIEVAL)

        # Actual ODA cost is max of usage-based or commitment-based
        effective_oda_cost = max(total_oda_cost, min_commitment_cost)
        savings_vs_ods = total_ods_cost - effective_oda_cost if total_ods_cost > 0 else 0

        return {
            "eligible": eligible,
            "triggers": triggers,
            "estimated_weekly_ods_cost": round(total_ods_cost, 2),
            "estimated_weekly_oda_cost": round(effective_oda_cost, 2),
            "estimated_weekly_savings": round(savings_vs_ods, 2),
            "min_commitment_weekly_cost": round(min_commitment_cost, 2),
            "recommendation": "ENABLE_ADVANTAGE" if eligible and savings_vs_ods > 0 else "STAY_ON_STANDARD",
        }
