"""Unit tests for kinesis_analyzer module."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add lambda directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "lambda"))

from kinesis_analyzer import KinesisAnalyzer


class TestRecommendMode:
    """Test the per-stream mode recommendation logic."""

    def setup_method(self):
        """Create an analyzer instance with mocked clients."""
        with patch("kinesis_analyzer.boto3.Session"):
            self.analyzer = KinesisAnalyzer(region="us-east-1")

    def _make_usage_summary(self, **overrides):
        """Helper to create a usage summary dict."""
        defaults = {
            "stream_name": "test-stream",
            "evaluation_period_days": 7,
            "avg_incoming_bytes_per_sec": 1_000_000,
            "max_incoming_bytes_per_sec": 3_000_000,
            "p99_incoming_bytes_per_sec": 2_800_000,
            "avg_outgoing_bytes_per_sec": 2_000_000,
            "max_outgoing_bytes_per_sec": 6_000_000,
            "avg_incoming_mibs": 0.95,
            "max_incoming_mibs": 2.86,
            "avg_outgoing_mibs": 1.91,
            "traffic_variability_ratio": 2.0,
            "write_throttle_rate": 0.0,
            "total_write_throttles": 0,
            "total_read_throttles": 0,
            "open_shard_count": 4,
            "retention_hours": 24,
            "efo_consumer_count": 0,
            "total_ingest_gb": 50.0,
            "total_retrieval_gb": 100.0,
            "efo_retrieval_gb": 0.0,
            "standard_retrieval_gb": 100.0,
            "total_records": 50_000_000,
            "avg_record_bytes": 1075.0,
            "ods_cost_estimate": 10.0,
            "oda_cost_estimate": 4.0,
            "provisioned_cost_estimate": 5.0,
            "on_demand_cost_estimate": 10.0,
            "provisioned_saving_ratio": 0.50,
        }
        defaults.update(overrides)
        return defaults

    def test_throttled_provisioned_recommends_on_demand(self):
        """Provisioned stream with throttling should switch to on-demand."""
        summary = self._make_usage_summary(
            write_throttle_rate=0.05,
            total_write_throttles=5000,
            total_read_throttles=200,
        )
        rec = self.analyzer.recommend_mode(summary, "PROVISIONED")
        assert rec["recommendation"] == "ON_DEMAND"
        assert rec["priority"] == "HIGH"
        assert "throttling" in rec["reasoning"].lower()

    def test_highly_variable_traffic_recommends_on_demand(self):
        """Highly variable traffic should use On-demand."""
        summary = self._make_usage_summary(traffic_variability_ratio=5.0)
        rec = self.analyzer.recommend_mode(summary, "ON_DEMAND")
        assert rec["recommendation"] == "ON_DEMAND"
        assert "variability" in rec["reasoning"].lower()

    def test_provisioned_stays_if_more_than_15_percent_cheaper(self):
        """Provisioned stays only when it's >15% cheaper and traffic is stable."""
        summary = self._make_usage_summary(
            traffic_variability_ratio=1.5,
            write_throttle_rate=0.0,
            provisioned_saving_ratio=0.20,
        )
        rec = self.analyzer.recommend_mode(summary, "PROVISIONED")
        assert rec["recommendation"] == "PROVISIONED"
        assert "20%" in rec["reasoning"]

    def test_provisioned_switches_if_saving_below_15_percent(self):
        """Provisioned should switch to on-demand if cost saving is <=15%."""
        summary = self._make_usage_summary(
            traffic_variability_ratio=1.5,
            write_throttle_rate=0.0,
            provisioned_saving_ratio=0.10,
        )
        rec = self.analyzer.recommend_mode(summary, "PROVISIONED")
        assert rec["recommendation"] == "ON_DEMAND"

    def test_default_recommends_on_demand(self):
        """When no strong signal, default to On-demand."""
        summary = self._make_usage_summary(
            traffic_variability_ratio=2.0,
            provisioned_saving_ratio=0.0,
        )
        rec = self.analyzer.recommend_mode(summary, "ON_DEMAND")
        assert rec["recommendation"] == "ON_DEMAND"

    def test_provisioned_with_variable_traffic_switches(self):
        """Even if provisioned is cheaper, variable traffic should go on-demand."""
        summary = self._make_usage_summary(
            traffic_variability_ratio=4.0,
            provisioned_saving_ratio=0.30,
        )
        rec = self.analyzer.recommend_mode(summary, "PROVISIONED")
        assert rec["recommendation"] == "ON_DEMAND"
        assert rec["priority"] == "MEDIUM"

    def test_no_advantage_in_per_stream_recommendation(self):
        """Per-stream recommendation should never return ON_DEMAND_ADVANTAGE."""
        summary = self._make_usage_summary(efo_consumer_count=10)
        rec = self.analyzer.recommend_mode(summary, "ON_DEMAND")
        assert "ADVANTAGE" not in rec["recommendation"]


class TestAdvantageAssessment:
    """Test the account-level Advantage eligibility logic."""

    def setup_method(self):
        with patch("kinesis_analyzer.boto3.Session"):
            self.analyzer = KinesisAnalyzer(region="us-east-1")

    def _make_stream_result(self, name, efo_count=0, ods_cost=5.0, oda_cost=2.0):
        return {
            "stream_name": name,
            "current_mode": "ON_DEMAND",
            "efo_consumer_count": efo_count,
            "metrics_summary": {
                "ods_cost_estimate": ods_cost,
                "oda_cost_estimate": oda_cost,
            },
        }

    def test_aggregate_throughput_triggers_advantage(self):
        """>=10 MiB/s aggregate triggers Advantage eligibility."""
        results = [self._make_stream_result("s1")]
        assessment = self.analyzer._assess_advantage_eligibility(12.0, 5, 0, results)
        assert assessment["eligible"] is True
        assert "10 MiB/s" in assessment["triggers"][0]

    def test_below_threshold_not_eligible(self):
        """<10 MiB/s with few streams and no EFO should not be eligible."""
        results = [self._make_stream_result("s1", efo_count=0)]
        assessment = self.analyzer._assess_advantage_eligibility(5.0, 5, 0, results)
        assert assessment["eligible"] is False

    def test_efo_consumers_trigger_advantage(self):
        """>2 EFO consumers on a stream triggers Advantage."""
        results = [self._make_stream_result("s1", efo_count=5)]
        assessment = self.analyzer._assess_advantage_eligibility(5.0, 5, 5, results)
        assert assessment["eligible"] is True
        assert "EFO" in assessment["triggers"][0]

    def test_more_than_50_streams_triggers_advantage(self):
        """>50 streams triggers Advantage."""
        results = [self._make_stream_result("s1")]
        assessment = self.analyzer._assess_advantage_eligibility(5.0, 51, 0, results)
        assert assessment["eligible"] is True
        assert "50" in assessment["triggers"][0]

    def test_minimum_commitment_factored_in(self):
        """If usage is below 25 MiB/s commitment, effective cost should reflect shortfall."""
        # Low usage but eligible via EFO
        results = [self._make_stream_result("s1", efo_count=5, ods_cost=1.0, oda_cost=0.5)]
        assessment = self.analyzer._assess_advantage_eligibility(5.0, 5, 5, results)
        # min commitment cost should be > the usage-based ODA cost
        assert assessment["min_commitment_weekly_cost"] > 0.5
        assert assessment["estimated_weekly_oda_cost"] >= assessment["min_commitment_weekly_cost"]


class TestComputeUsageSummary:
    """Test the cost calculation logic."""

    def setup_method(self):
        with patch("kinesis_analyzer.boto3.Session"):
            self.analyzer = KinesisAnalyzer(region="us-east-1")

    def _make_metrics(self, incoming_bytes_per_hour, hours=168, records_per_hour=1000):
        from datetime import datetime, timezone
        datapoints = []
        for i in range(hours):
            datapoints.append({
                "Timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "Sum": incoming_bytes_per_hour,
            })
        return {
            "IncomingBytes": datapoints,
            "IncomingRecords": [{"Timestamp": d["Timestamp"], "Sum": records_per_hour} for d in datapoints],
            "OutgoingBytes": [{"Timestamp": d["Timestamp"], "Sum": incoming_bytes_per_hour} for d in datapoints],
            "OutgoingRecords": [{"Timestamp": d["Timestamp"], "Sum": records_per_hour} for d in datapoints],
            "WriteProvisionedThroughputExceeded": [{"Timestamp": d["Timestamp"], "Sum": 0} for d in datapoints],
            "ReadProvisionedThroughputExceeded": [{"Timestamp": d["Timestamp"], "Sum": 0} for d in datapoints],
        }

    def test_avg_uses_full_period_not_datapoint_count(self):
        """Average throughput must be computed over full 7-day period."""
        # Only 24 hours of data (1 day out of 7)
        metrics = self._make_metrics(1024**3, hours=24)  # 1 GB per hour for 24 hours
        desc = {"OpenShardCount": 4, "RetentionPeriodHours": 24}
        summary = self.analyzer.compute_usage_summary("test", metrics, desc, efo_consumer_count=0)
        # Total = 24 GB. Full period = 7*24*3600 = 604800 seconds.
        # Avg = 24 * 1024^3 / 604800 ≈ 42,613 bytes/sec ≈ 0.0406 MiB/s
        # NOT 24 * 1024^3 / (24*3600) ≈ 298,491 bytes/sec ≈ 0.2847 MiB/s
        assert summary["avg_incoming_mibs"] < 0.05  # should be ~0.04, not ~0.28

    def test_ods_cost_includes_per_stream_charge(self):
        """On-demand Standard cost must include per-stream hourly charge."""
        metrics = self._make_metrics(1024**3, hours=168)
        desc = {"OpenShardCount": 4, "RetentionPeriodHours": 24}
        summary = self.analyzer.compute_usage_summary("test", metrics, desc, efo_consumer_count=0)
        assert summary["ods_cost_estimate"] > 6.72  # at least stream charge (168 * $0.04)

    def test_oda_has_no_per_stream_charge(self):
        """On-demand Advantage cost should not include per-stream charge."""
        metrics = self._make_metrics(1024**3, hours=168)
        desc = {"OpenShardCount": 4, "RetentionPeriodHours": 24}
        summary = self.analyzer.compute_usage_summary("test", metrics, desc, efo_consumer_count=0)
        assert summary["oda_cost_estimate"] < summary["ods_cost_estimate"]

    def test_efo_increases_provisioned_cost(self):
        """EFO consumers should add cost to provisioned estimate."""
        metrics = self._make_metrics(1024**3, hours=168)
        desc = {"OpenShardCount": 4, "RetentionPeriodHours": 24}
        cost_no_efo = self.analyzer.compute_usage_summary("test", metrics, desc, efo_consumer_count=0)
        cost_with_efo = self.analyzer.compute_usage_summary("test", metrics, desc, efo_consumer_count=2)
        assert cost_with_efo["provisioned_cost_estimate"] > cost_no_efo["provisioned_cost_estimate"]

    def test_extended_retention_increases_ods_cost(self):
        """Extended retention (>24h) should increase On-demand Standard cost."""
        metrics = self._make_metrics(1024**3, hours=168)
        desc_24h = {"OpenShardCount": 4, "RetentionPeriodHours": 24}
        desc_7d = {"OpenShardCount": 4, "RetentionPeriodHours": 168}
        cost_24h = self.analyzer.compute_usage_summary("test", metrics, desc_24h, efo_consumer_count=0)
        cost_7d = self.analyzer.compute_usage_summary("test", metrics, desc_7d, efo_consumer_count=0)
        assert cost_7d["ods_cost_estimate"] > cost_24h["ods_cost_estimate"]

    def test_provisioned_put_units_calculated(self):
        """Provisioned cost should include PUT Payload Units."""
        metrics = self._make_metrics(
            incoming_bytes_per_hour=100 * 35 * 1024,
            hours=168,
            records_per_hour=100,
        )
        desc = {"OpenShardCount": 4, "RetentionPeriodHours": 24}
        summary = self.analyzer.compute_usage_summary("test", metrics, desc, efo_consumer_count=0)
        shard_only_cost = 4 * 0.015 * 168
        assert summary["provisioned_cost_estimate"] > shard_only_cost


class TestListStreamConsumers:
    """Test EFO consumer listing."""

    def setup_method(self):
        with patch("kinesis_analyzer.boto3.Session"):
            self.analyzer = KinesisAnalyzer(region="us-east-1")

    def test_list_stream_consumers_returns_consumers(self):
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Consumers": [
                    {"ConsumerName": "app1", "ConsumerARN": "arn:aws:kinesis:us-east-1:123:stream/test/consumer/app1", "ConsumerStatus": "ACTIVE"},
                    {"ConsumerName": "app2", "ConsumerARN": "arn:aws:kinesis:us-east-1:123:stream/test/consumer/app2", "ConsumerStatus": "ACTIVE"},
                ]
            }
        ]
        self.analyzer.kinesis_client.get_paginator.return_value = mock_paginator
        result = self.analyzer.list_stream_consumers("arn:aws:kinesis:us-east-1:123:stream/test")
        assert len(result) == 2
        assert result[0]["ConsumerName"] == "app1"

    def test_list_stream_consumers_handles_error(self):
        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = Exception("Access denied")
        self.analyzer.kinesis_client.get_paginator.return_value = mock_paginator
        result = self.analyzer.list_stream_consumers("arn:aws:kinesis:us-east-1:123:stream/test")
        assert result == []
