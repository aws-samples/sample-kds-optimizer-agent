"""
AWS Lambda handler for the Bedrock Agent action group.

This Lambda is invoked by the Bedrock Agent to analyze Kinesis Data Streams
and generate optimization reports.
"""

import json
import logging
import os

from kinesis_analyzer import KinesisAnalyzer
from report_generator import ReportGenerator

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REPORT_BUCKET = os.environ.get("REPORT_BUCKET_NAME", "")


def lambda_handler(event, context):
    """
    Bedrock Agent action group Lambda handler.

    Handles the following API paths:
    - /analyzeStreams: Analyze all KDS streams and return recommendations
    - /generateReport: Analyze streams and store report in S3
    - /getStreamDetails: Get detailed metrics for a specific stream
    """
    logger.info(f"Received event: {json.dumps(event)}")

    # Parse Bedrock Agent event
    api_path = event.get("apiPath", "")
    http_method = event.get("httpMethod", "GET")
    parameters = event.get("parameters", [])
    request_body = event.get("requestBody", {})

    # Convert parameters list to dict
    params = {}
    if parameters:
        for param in parameters:
            params[param["name"]] = param["value"]

    # Also extract from request body if present
    if request_body:
        content = request_body.get("content", {})
        app_json = content.get("application/json", {})
        if "properties" in app_json:
            for prop in app_json["properties"]:
                params[prop["name"]] = prop["value"]

    try:
        if api_path == "/analyzeStreams":
            result = handle_analyze_streams(params)
        elif api_path == "/generateReport":
            result = handle_generate_report(params)
        elif api_path == "/getStreamDetails":
            result = handle_get_stream_details(params)
        else:
            result = {"error": f"Unknown API path: {api_path}"}

        response_body = {"application/json": {"body": json.dumps(result, default=str)}}

        action_response = {
            "actionGroup": event.get("actionGroup", ""),
            "apiPath": api_path,
            "httpMethod": http_method,
            "httpStatusCode": 200,
            "responseBody": response_body,
        }

        api_response = {"messageVersion": "1.0", "response": action_response}
        return api_response

    except Exception as e:
        logger.error(f"Error handling request: {e}", exc_info=True)
        error_body = {"application/json": {"body": json.dumps({"error": str(e)})}}
        return {
            "messageVersion": "1.0",
            "response": {
                "actionGroup": event.get("actionGroup", ""),
                "apiPath": api_path,
                "httpMethod": http_method,
                "httpStatusCode": 500,
                "responseBody": error_body,
            },
        }


def handle_analyze_streams(params: dict) -> dict:
    """Analyze all streams and return recommendations without storing a report."""
    analyzer = KinesisAnalyzer()
    results = analyzer.analyze_all_streams()
    return results


def handle_generate_report(params: dict) -> dict:
    """Analyze all streams and store the report in S3."""
    bucket = params.get("bucket_name", REPORT_BUCKET)
    if not bucket:
        return {"error": "No S3 bucket specified. Set REPORT_BUCKET_NAME env var or pass bucket_name parameter."}

    analyzer = KinesisAnalyzer()
    analysis_results = analyzer.analyze_all_streams()

    generator = ReportGenerator(bucket_name=bucket)
    report_summary = generator.generate_and_store(analysis_results)
    return report_summary


def handle_get_stream_details(params: dict) -> dict:
    """Get detailed metrics and recommendation for a specific stream."""
    stream_name = params.get("stream_name")
    if not stream_name:
        return {"error": "stream_name parameter is required."}

    analyzer = KinesisAnalyzer()

    try:
        stream_desc = analyzer.describe_stream(stream_name)
        metrics = analyzer.get_stream_metrics(stream_name)

        # Get EFO consumers
        stream_arn = stream_desc.get("StreamARN", "")
        consumers = analyzer.list_stream_consumers(stream_arn) if stream_arn else []
        efo_consumer_count = len(consumers)

        usage_summary = analyzer.compute_usage_summary(
            stream_name, metrics, stream_desc, efo_consumer_count=efo_consumer_count
        )

        current_mode = stream_desc.get("StreamModeDetails", {}).get(
            "StreamMode", "PROVISIONED"
        )

        # For single-stream analysis, use its own throughput as aggregate estimate
        rec = analyzer.recommend_mode(usage_summary, current_mode)

        return {
            "stream_name": stream_name,
            "current_mode": current_mode,
            "current_shards": stream_desc.get("OpenShardCount", 0),
            "stream_arn": stream_arn,
            "retention_period_hours": stream_desc.get("RetentionPeriodHours", 24),
            "efo_consumers": consumers,
            "efo_consumer_count": efo_consumer_count,
            "metrics_summary": usage_summary,
            "recommendation": rec["recommendation"],
            "reasoning": rec["reasoning"],
            "priority": rec["priority"],
        }
    except Exception as e:
        return {"error": f"Failed to analyze stream '{stream_name}': {str(e)}"}
