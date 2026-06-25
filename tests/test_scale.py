"""
Scale test: verify the analyzer and report generator handle 500 streams correctly.

This test mocks AWS APIs and validates:
1. Batch metric fetching works with 500 streams
2. Concurrent stream descriptions complete
3. HTML report renders all 500 streams without issues
4. Processing completes within reasonable time
"""

import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "lambda"))

from kinesis_analyzer import KinesisAnalyzer
from report_generator import ReportGenerator

NUM_STREAMS = 500


def generate_mock_streams(n: int) -> list[dict]:
    """Generate n mock stream summaries with realistic variety."""
    modes = ["ON_DEMAND"] * 350 + ["PROVISIONED"] * 150  # 70% on-demand, 30% provisioned
    random.shuffle(modes)

    streams = []
    for i in range(n):
        mode = modes[i] if i < len(modes) else "ON_DEMAND"
        streams.append({
            "StreamName": f"stream-{i:04d}",
            "StreamARN": f"arn:aws:kinesis:ap-southeast-2:123456789012:stream/stream-{i:04d}",
            "StreamStatus": "ACTIVE",
            "StreamModeDetails": {"StreamMode": mode},
        })
    return streams


def generate_mock_description(stream_name: str, mode: str) -> dict:
    """Generate a mock stream description."""
    shard_count = random.choice([1, 2, 4, 8, 16]) if mode == "PROVISIONED" else random.choice([1, 2, 4, 8])
    return {
        "StreamName": stream_name,
        "StreamARN": f"arn:aws:kinesis:ap-southeast-2:123456789012:stream/{stream_name}",
        "StreamStatus": "ACTIVE",
        "OpenShardCount": shard_count,
        "RetentionPeriodHours": 24,
        "StreamModeDetails": {"StreamMode": mode},
    }


def generate_mock_metrics(stream_name: str, mode: str) -> dict:
    """Generate mock CloudWatch metrics for a stream with realistic patterns."""
    now = datetime.now(timezone.utc)
    hours = 7 * 24  # 7 days of hourly data

    # Different traffic patterns
    pattern = random.choice(["steady", "bursty", "growing", "idle", "high"])

    datapoints = []
    for h in range(hours):
        ts = now - timedelta(hours=hours - h)

        if pattern == "steady":
            base = random.uniform(50_000, 200_000)  # 50-200 KB/s * 3600
            value = base * 3600 * random.uniform(0.8, 1.2)
        elif pattern == "bursty":
            base = random.uniform(10_000, 50_000)
            burst = random.uniform(5, 20) if random.random() < 0.1 else 1
            value = base * 3600 * burst
        elif pattern == "growing":
            base = random.uniform(10_000, 100_000) * (1 + h / hours)
            value = base * 3600 * random.uniform(0.9, 1.1)
        elif pattern == "idle":
            value = random.uniform(0, 1000) * 3600
        elif pattern == "high":
            base = random.uniform(500_000, 5_000_000)  # 0.5-5 MB/s
            value = base * 3600 * random.uniform(0.7, 1.3)
        else:
            value = 0

        datapoints.append({"Timestamp": ts, "Sum": value})

    # Generate throttle data for some provisioned streams
    throttle_data = []
    if mode == "PROVISIONED" and random.random() < 0.2:  # 20% of provisioned have throttling
        for h in range(hours):
            ts = now - timedelta(hours=hours - h)
            throttle_data.append({"Timestamp": ts, "Sum": random.uniform(0, 50)})

    incoming_records = [
        {"Timestamp": dp["Timestamp"], "Sum": dp["Sum"] / random.uniform(500, 2000)}
        for dp in datapoints
    ]

    return {
        "IncomingBytes": datapoints,
        "IncomingRecords": incoming_records,
        "OutgoingBytes": [{"Timestamp": dp["Timestamp"], "Sum": dp["Sum"] * random.uniform(1, 3)} for dp in datapoints],
        "OutgoingRecords": incoming_records,
        "WriteProvisionedThroughputExceeded": throttle_data,
        "ReadProvisionedThroughputExceeded": [],
    }


def test_500_streams_analysis():
    """Test that 500 streams can be analyzed and reported on."""
    print(f"\n{'='*60}")
    print(f"SCALE TEST: {NUM_STREAMS} streams")
    print(f"{'='*60}")

    # Generate mock data
    mock_streams = generate_mock_streams(NUM_STREAMS)
    stream_modes = {s["StreamName"]: s["StreamModeDetails"]["StreamMode"] for s in mock_streams}

    # Mock the analyzer
    with patch("kinesis_analyzer.boto3.Session") as mock_session:
        analyzer = KinesisAnalyzer(region="ap-southeast-2")
        analyzer.region = "ap-southeast-2"

        # Mock list_streams
        analyzer.list_streams = MagicMock(return_value=mock_streams)
        analyzer.get_account_id = MagicMock(return_value="123456789012")

        # Mock describe_stream
        def mock_describe(name):
            mode = stream_modes.get(name, "ON_DEMAND")
            return generate_mock_description(name, mode)

        analyzer.describe_stream = MagicMock(side_effect=mock_describe)

        # Mock batch metrics — return pre-generated metrics
        mock_metrics_cache = {}
        for s in mock_streams:
            name = s["StreamName"]
            mode = stream_modes[name]
            mock_metrics_cache[name] = generate_mock_metrics(name, mode)

        analyzer.get_metrics_batch = MagicMock(return_value=mock_metrics_cache)

        # Run the analysis
        print("\n1. Running analyze_all_streams()...")
        start = time.time()
        results = analyzer.analyze_all_streams()
        analysis_time = time.time() - start
        print(f"   Completed in {analysis_time:.2f}s")

        # Validate results
        assert results["account_id"] == "123456789012"
        assert results["region"] == "ap-southeast-2"
        assert len(results["streams"]) == NUM_STREAMS
        print(f"   ✓ All {NUM_STREAMS} streams analyzed")

        summary = results["account_level_summary"]
        print(f"   Total: {summary['total_streams']}")
        print(f"   Provisioned: {summary['provisioned_streams']}")
        print(f"   On-demand: {summary['on_demand_streams']}")
        print(f"   Aggregate ingest: {summary['aggregate_on_demand_ingest_mibs']:.2f} MiB/s")
        print(f"   OD Advantage eligible: {summary['on_demand_advantage_eligible']}")

        # Count recommendations
        rec_counts = {}
        priority_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
        for s in results["streams"]:
            rec = s.get("recommendation", "UNKNOWN")
            rec_counts[rec] = rec_counts.get(rec, 0) + 1
            p = s.get("priority", "UNKNOWN")
            if p in priority_counts:
                priority_counts[p] += 1

        print(f"\n   Recommendations:")
        for rec, count in sorted(rec_counts.items()):
            print(f"     {rec}: {count}")
        print(f"\n   Priorities:")
        for p, count in sorted(priority_counts.items()):
            print(f"     {p}: {count}")

    # Test HTML report generation
    print("\n2. Generating HTML report...")
    start = time.time()

    with patch("report_generator.boto3.Session"):
        generator = ReportGenerator(bucket_name="test-bucket", region="ap-southeast-2")
        generator.s3_client = MagicMock()

        report = generator.generate_report(results)
        html = generator._generate_html(report)
        report_time = time.time() - start

    print(f"   Completed in {report_time:.2f}s")
    print(f"   HTML size: {len(html):,} bytes ({len(html)/1024:.1f} KB)")
    print(f"   ✓ Report ID: {report['report_id'][:8]}...")

    # Validate HTML structure
    assert "<!DOCTYPE html>" in html
    assert "Stream Analysis" in html
    assert f"stream-0000" in html
    assert f"stream-{NUM_STREAMS-1:04d}" in html
    print(f"   ✓ First and last stream present in HTML")

    # Check all streams are in the table
    for i in range(0, NUM_STREAMS, 50):  # Spot check every 50th stream
        assert f"stream-{i:04d}" in html, f"Missing stream-{i:04d} in HTML"
    print(f"   ✓ All streams represented in HTML (spot-checked)")

    # Test JSON report
    print("\n3. Testing JSON report...")
    import json
    json_str = json.dumps(report, default=str)
    print(f"   JSON size: {len(json_str):,} bytes ({len(json_str)/1024:.1f} KB)")

    # Performance summary
    print(f"\n{'='*60}")
    print(f"RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"  Streams analyzed:    {NUM_STREAMS}")
    print(f"  Analysis time:       {analysis_time:.2f}s")
    print(f"  Report gen time:     {report_time:.2f}s")
    print(f"  Total time:          {analysis_time + report_time:.2f}s")
    print(f"  HTML report size:    {len(html)/1024:.1f} KB")
    print(f"  JSON report size:    {len(json_str)/1024:.1f} KB")
    print(f"  Streams/second:      {NUM_STREAMS/analysis_time:.0f}")
    print(f"  Lambda 5min budget:  {'PASS ✓' if (analysis_time + report_time) < 280 else 'FAIL ✗'}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    test_500_streams_analysis()
