"""
Report generator for Kinesis Data Streams mode optimization.

Formats analysis results and stores them in S3 as both JSON (programmatic) and HTML (human-readable).
"""

import json
import logging
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ReportGenerator:
    """Generates and stores optimization reports in S3."""

    def __init__(self, bucket_name: str, region: str | None = None):
        self.bucket_name = bucket_name
        session = boto3.Session(region_name=region)
        self.s3_client = session.client("s3")

    def generate_report(self, analysis_results: dict) -> dict:
        """Create a structured report from analysis results."""
        report = {
            "report_id": str(uuid.uuid4()),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "region": analysis_results["region"],
            "account_id": analysis_results["account_id"],
            "streams": analysis_results["streams"],
            "account_level_summary": analysis_results["account_level_summary"],
            "recommendations_summary": self._build_recommendations_summary(
                analysis_results["streams"]
            ),
        }
        return report

    def _build_recommendations_summary(self, streams: list[dict]) -> dict:
        """Summarize recommendations across all streams."""
        recommendations = {}
        for stream in streams:
            rec = stream.get("recommendation", "UNKNOWN")
            if rec not in recommendations:
                recommendations[rec] = []
            recommendations[rec].append(stream.get("stream_name", "unknown"))

        high_priority = [
            s for s in streams if s.get("priority") == "HIGH"
        ]

        return {
            "by_recommendation": recommendations,
            "high_priority_actions": [
                {
                    "stream_name": s["stream_name"],
                    "current_mode": s.get("current_mode"),
                    "recommended_mode": s.get("recommendation"),
                    "reasoning": s.get("reasoning"),
                }
                for s in high_priority
            ],
            "total_recommendations": len(streams),
            "streams_needing_change": sum(
                1
                for s in streams
                if s.get("recommendation")
                and s.get("recommendation") != s.get("current_mode")
                and s.get("recommendation") != "UNABLE_TO_ASSESS"
            ),
        }

    def _generate_html(self, report: dict) -> str:
        """Generate a clean, readable HTML report."""
        streams = report["streams"]
        summary = report["account_level_summary"]
        rec_summary = report["recommendations_summary"]

        # Separate streams by action needed
        needs_change = [s for s in streams if s.get("recommendation") and s.get("recommendation") != s.get("current_mode") and s.get("recommendation") != "UNABLE_TO_ASSESS"]
        no_change = [s for s in streams if s.get("recommendation") == s.get("current_mode")]
        errors = [s for s in streams if s.get("recommendation") == "UNABLE_TO_ASSESS"]

        # Build stream rows for the main table
        stream_rows = ""
        for s in sorted(streams, key=lambda x: ({"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(x.get("priority", "LOW"), 3))):
            priority = s.get("priority", "N/A")
            priority_class = {"HIGH": "priority-high", "MEDIUM": "priority-medium", "LOW": "priority-low"}.get(priority, "")
            current = self._format_mode(s.get("current_mode", "N/A"))
            recommended = self._format_mode(s.get("recommendation", "N/A"))
            change_needed = s.get("recommendation") != s.get("current_mode") and s.get("recommendation") != "UNABLE_TO_ASSESS"
            change_icon = "&#x26A0;&#xFE0F;" if change_needed else "&#x2705;"

            metrics = s.get("metrics_summary", {})
            avg_mibs = metrics.get("avg_incoming_mibs", 0)
            max_mibs = metrics.get("max_incoming_mibs", 0)
            variability = metrics.get("traffic_variability_ratio", 0)
            provisioned_saving = metrics.get("provisioned_saving_ratio", 0)
            cost_note = f"{provisioned_saving:.0%} cheaper" if provisioned_saving > 0 else "On-demand preferred"

            stream_rows += f"""
            <tr>
                <td><strong>{s.get('stream_name', 'N/A')}</strong></td>
                <td>{current}</td>
                <td>{change_icon} {recommended}</td>
                <td class="{priority_class}">{priority}</td>
                <td>{avg_mibs:.3f}</td>
                <td>{max_mibs:.3f}</td>
                <td>{variability:.1f}x</td>
                <td>{cost_note}</td>
            </tr>"""

        # Build high-priority actions section
        high_priority_html = ""
        if rec_summary["high_priority_actions"]:
            high_priority_html = """
            <div class="alert alert-high">
                <h3>&#x1F6A8; High Priority Actions Required</h3>
                <ul>"""
            for action in rec_summary["high_priority_actions"]:
                high_priority_html += f"""
                    <li><strong>{action['stream_name']}</strong>: {action['current_mode']} &rarr; {action['recommended_mode']}<br>
                    <em>{action['reasoning']}</em></li>"""
            high_priority_html += """
                </ul>
            </div>"""

        # Build reasoning details
        reasoning_rows = ""
        for s in streams:
            if s.get("reasoning"):
                reasoning_rows += f"""
                <tr>
                    <td><strong>{s.get('stream_name', 'N/A')}</strong></td>
                    <td>{s.get('reasoning', '')}</td>
                </tr>"""

        advantage_status = "Eligible &#x2705;" if summary.get("on_demand_advantage_eligible") else "Not Eligible"
        advantage_class = "stat-good" if summary.get("on_demand_advantage_eligible") else "stat-neutral"

        # Build On-demand Advantage assessment section
        advantage_assessment = summary.get("advantage_assessment", {})
        advantage_section_html = ""
        if advantage_assessment:
            adv_eligible = advantage_assessment.get("eligible", False)
            adv_triggers = advantage_assessment.get("triggers", [])
            adv_recommendation = advantage_assessment.get("recommendation", "")
            adv_ods_cost = advantage_assessment.get("estimated_weekly_ods_cost", 0)
            adv_oda_cost = advantage_assessment.get("estimated_weekly_oda_cost", 0)
            adv_savings = advantage_assessment.get("estimated_weekly_savings", 0)
            adv_min_cost = advantage_assessment.get("min_commitment_weekly_cost", 0)

            if adv_eligible:
                triggers_html = "".join(f"<li>{t}</li>" for t in adv_triggers)
                adv_badge = "&#x2705; Enable On-demand Advantage" if adv_recommendation == "ENABLE_ADVANTAGE" else "&#x26A0;&#xFE0F; Eligible — review usage patterns"
                advantage_section_html = f"""
        <div class="section">
            <h2>&#x1F4A1; Account-Level Recommendation: On-demand Advantage</h2>
            <p style="margin-bottom:1rem;">On-demand Advantage is an <strong>account-level setting</strong> that applies to all on-demand streams in this region. It is not a per-stream action.</p>
            <p style="margin-bottom:1rem;"><strong>Recommendation: {adv_badge}</strong></p>
            <h3 style="font-size:1rem; margin-bottom:0.5rem;">Why Advantage is recommended:</h3>
            <ul style="padding-left:1.5rem; margin-bottom:1rem;">
                {triggers_html}
            </ul>
            <p style="margin-top:1rem; font-size:0.85rem; color:#666;">
                <strong>Action:</strong> First switch provisioned streams to on-demand (per-stream actions above), then enable On-demand Advantage at the account level for this region.
                Advantage has a 25 MiB/s minimum commitment and a 24-hour minimum enablement period.
            </p>
        </div>"""
            else:
                advantage_section_html = f"""
        <div class="section">
            <h2>&#x1F4A1; Account-Level: On-demand Advantage</h2>
            <p>On-demand Advantage is <strong>not recommended</strong> at this time. None of the eligibility triggers are met:</p>
            <ul style="padding-left:1.5rem; margin-top:0.5rem;">
                <li>Aggregate throughput &lt; 10 MiB/s</li>
                <li>No stream has more than 2 EFO consumers</li>
                <li>Account has 50 or fewer streams</li>
            </ul>
        </div>"""

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KDS Mode Optimization Report - {report['region']} - {report['generated_at'][:10]}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f7fa; color: #1a1a2e; line-height: 1.6; padding: 2rem; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #232f3e, #37475a); color: white; padding: 2rem; border-radius: 12px; margin-bottom: 2rem; }}
        .header h1 {{ font-size: 1.8rem; margin-bottom: 0.5rem; }}
        .header .meta {{ opacity: 0.85; font-size: 0.9rem; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
        .stat-card {{ background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; }}
        .stat-card .value {{ font-size: 2rem; font-weight: 700; color: #232f3e; }}
        .stat-card .label {{ font-size: 0.85rem; color: #666; margin-top: 0.25rem; }}
        .stat-good .value {{ color: #1a8754; }}
        .stat-warning .value {{ color: #d4760a; }}
        .stat-neutral .value {{ color: #5a6570; }}
        .section {{ background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 1.5rem; margin-bottom: 1.5rem; }}
        .section h2 {{ font-size: 1.3rem; margin-bottom: 1rem; color: #232f3e; border-bottom: 2px solid #f0f0f0; padding-bottom: 0.5rem; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
        th {{ background: #f8f9fa; text-align: left; padding: 0.75rem; border-bottom: 2px solid #dee2e6; font-weight: 600; color: #495057; }}
        td {{ padding: 0.75rem; border-bottom: 1px solid #f0f0f0; }}
        tr:hover {{ background: #f8f9fa; }}
        .priority-high {{ color: #dc3545; font-weight: 700; }}
        .priority-medium {{ color: #d4760a; font-weight: 600; }}
        .priority-low {{ color: #6c757d; }}
        .alert {{ border-radius: 8px; padding: 1.5rem; margin-bottom: 1.5rem; }}
        .alert-high {{ background: #fff5f5; border-left: 4px solid #dc3545; }}
        .alert-high h3 {{ color: #dc3545; margin-bottom: 0.75rem; }}
        .alert-high ul {{ padding-left: 1.5rem; }}
        .alert-high li {{ margin-bottom: 0.75rem; }}
        .mode-badge {{ display: inline-block; padding: 0.2rem 0.6rem; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }}
        .mode-on-demand {{ background: #d4edda; color: #155724; }}
        .mode-advantage {{ background: #cce5ff; color: #004085; }}
        .mode-provisioned {{ background: #fff3cd; color: #856404; }}
        .footer {{ text-align: center; color: #999; font-size: 0.8rem; margin-top: 2rem; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Kinesis Data Streams Mode Optimization Report</h1>
            <div class="meta">
                Account: {report['account_id']} &nbsp;|&nbsp; Region: {report['region']} &nbsp;|&nbsp;
                Generated: {report['generated_at'][:19].replace('T', ' ')} UTC &nbsp;|&nbsp;
                Report ID: {report['report_id'][:8]}...
            </div>
        </div>

        {high_priority_html}

        {advantage_section_html}

        <div class="stats-grid">
            <div class="stat-card">
                <div class="value">{summary.get('total_streams', 0)}</div>
                <div class="label">Total Streams</div>
            </div>
            <div class="stat-card stat-warning">
                <div class="value">{rec_summary.get('streams_needing_change', 0)}</div>
                <div class="label">Need Mode Change</div>
            </div>
            <div class="stat-card">
                <div class="value">{summary.get('provisioned_streams', 0)}</div>
                <div class="label">Provisioned</div>
            </div>
            <div class="stat-card">
                <div class="value">{summary.get('on_demand_streams', 0)}</div>
                <div class="label">On-Demand</div>
            </div>
            <div class="stat-card {advantage_class}">
                <div class="value" style="font-size:1rem;">{advantage_status}</div>
                <div class="label">On-Demand Advantage</div>
            </div>
            <div class="stat-card">
                <div class="value">{summary.get('aggregate_all_ingest_mibs', summary.get('aggregate_on_demand_ingest_mibs', 0)):.2f}</div>
                <div class="label">Aggregate Ingest (MiB/s)</div>
            </div>
        </div>

        <div class="section">
            <h2>Stream Analysis</h2>
            <table>
                <thead>
                    <tr>
                        <th>Stream Name</th>
                        <th>Current Mode</th>
                        <th>Recommended</th>
                        <th>Priority</th>
                        <th>Avg Ingest (MiB/s)</th>
                        <th>Peak Ingest (MiB/s)</th>
                        <th>Variability</th>
                        <th>Cost Comparison</th>
                    </tr>
                </thead>
                <tbody>
                    {stream_rows}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>Detailed Reasoning</h2>
            <table>
                <thead>
                    <tr>
                        <th style="width:200px;">Stream</th>
                        <th>Recommendation Reasoning</th>
                    </tr>
                </thead>
                <tbody>
                    {reasoning_rows}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>Mode Selection Criteria</h2>
            <table>
                <thead>
                    <tr><th>Mode</th><th>When to Use</th><th>Key Benefit</th></tr>
                </thead>
                <tbody>
                    <tr><td><span class="mode-badge mode-advantage">On-Demand Advantage</span></td><td>Account aggregate throughput &ge; 10 MiB/s across on-demand streams</td><td>60%+ lower pricing, warm throughput, no per-stream charge, up to 50 EFO consumers</td></tr>
                    <tr><td><span class="mode-badge mode-on-demand">On-Demand Standard</span></td><td>Variable/unpredictable traffic, or when provisioned savings &lt; 15%</td><td>Zero capacity planning, automatic scaling, no throttling risk</td></tr>
                    <tr><td><span class="mode-badge mode-provisioned">Provisioned</span></td><td>Stable traffic AND provisioned is &gt;15% cheaper than on-demand</td><td>Cost effective for highly predictable, steady workloads</td></tr>
                </tbody>
            </table>
        </div>

        <div class="footer">
            &#x1F916; AI-Generated Analysis | Powered by Amazon Bedrock Agent<br>
            Analysis covers the past 7 days of CloudWatch metrics
        </div>
    </div>
</body>
</html>"""
        return html

    def _format_mode(self, mode: str) -> str:
        """Format mode name as a styled badge."""
        mode_map = {
            "PROVISIONED": '<span class="mode-badge mode-provisioned">Provisioned</span>',
            "ON_DEMAND": '<span class="mode-badge mode-on-demand">On-Demand Standard</span>',
            "ON_DEMAND_STANDARD": '<span class="mode-badge mode-on-demand">On-Demand Standard</span>',
            "ON_DEMAND_ADVANTAGE": '<span class="mode-badge mode-advantage">On-Demand Advantage</span>',
            "UNABLE_TO_ASSESS": '<span class="mode-badge" style="background:#f0f0f0;color:#666;">Unable to Assess</span>',
        }
        return mode_map.get(mode, mode)

    def store_report(self, report: dict) -> dict:
        """Store both JSON and HTML reports in S3. Returns dict with both keys."""
        timestamp = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
        base_key = f"kds-optimization-reports/{timestamp}-{report['report_id']}"

        # Store JSON
        json_key = f"{base_key}.json"
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=json_key,
            Body=json.dumps(report, indent=2, default=str),
            ContentType="application/json",
        )

        # Store HTML
        html_key = f"{base_key}.html"
        html_content = self._generate_html(report)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=html_key,
            Body=html_content,
            ContentType="text/html",
        )

        logger.info(f"Reports stored at s3://{self.bucket_name}/{base_key}.[json|html]")
        return {"json_key": json_key, "html_key": html_key}

    def generate_and_store(self, analysis_results: dict) -> dict:
        """Full pipeline: generate report and store in S3."""
        report = self.generate_report(analysis_results)
        keys = self.store_report(report)
        return {
            "report_id": report["report_id"],
            "s3_bucket": self.bucket_name,
            "s3_key_json": keys["json_key"],
            "s3_key_html": keys["html_key"],
            "generated_at": report["generated_at"],
            "total_streams_analyzed": len(report["streams"]),
            "streams_needing_change": report["recommendations_summary"]["streams_needing_change"],
            "high_priority_actions": len(
                report["recommendations_summary"]["high_priority_actions"]
            ),
        }
