# How the KDS Mode Optimizer Agent Works

## The Flow

```
You (or EventBridge schedule)
    │
    ▼
Amazon Bedrock Agent (AI brain)
    │  "Analyze streams and generate a report"
    ▼
Lambda Function (action group)
    │
    ├──► Step 1: List all streams (Kinesis API)
    ├──► Step 2: Describe each stream (get shard count, mode, retention)
    ├──► Step 3: List EFO consumers per stream
    ├──► Step 4: Pull 7 days of CloudWatch metrics
    ├──► Step 5: Crunch numbers per stream (three-way cost comparison)
    ├──► Step 6: Per-stream recommendation (provisioned vs on-demand)
    ├──► Step 7: Account-level On-demand Advantage assessment
    ├──► Step 8: Generate HTML + JSON report
    └──► Step 9: Store in S3
    │
    ▼
Bedrock Agent summarizes the result back to you
```

## Step by Step

### Step 1: Find all streams

Calls `kinesis:ListStreams` — gets every Kinesis Data Stream in the account/region. Returns names + current mode (on-demand or provisioned).

### Step 2: Describe each stream

For each stream, calls `kinesis:DescribeStreamSummary` to get the **shard count**, **retention period**, and **stream ARN**. Shard count is needed for provisioned cost calculations, retention period affects storage cost estimates, and the ARN is used to query EFO consumers.

Uses 20 concurrent threads to handle accounts with hundreds of streams.

### Step 3: List EFO consumers

For each stream, calls `kinesis:ListStreamConsumers` to discover registered Enhanced Fan-Out consumers. This data feeds into:
- Per-stream EFO cost calculations (consumer-shard-hours for provisioned, per-GB retrieval rates)
- Account-level Advantage eligibility (>2 EFO consumers triggers Advantage recommendation)

Runs concurrently across all streams.

### Step 4: Pull CloudWatch metrics (past 7 days)

For each stream, fetches these metrics at 1-hour granularity:

- **IncomingBytes** — how much data is being written
- **IncomingRecords** — how many records per second
- **OutgoingBytes** — how much consumers are reading
- **WriteProvisionedThroughputExceeded** — throttle events (writes rejected)
- **ReadProvisionedThroughputExceeded** — throttle events (reads rejected)

For large accounts (up to 500 streams), this uses the batch `GetMetricData` API (500 queries per call) instead of individual calls.

### Step 5: Crunch the numbers

For each stream, it computes:

| Metric | What it means |
|--------|---------------|
| **Avg throughput (MiB/s)** | Sustained data rate over the full 7-day period (total bytes / 7 days) |
| **Peak throughput (MiB/s)** | Highest hourly burst seen |
| **Traffic variability** | Ratio of peak to average. High = unpredictable traffic |
| **Three-way cost estimate** | Estimated cost under On-demand Standard, On-demand Advantage, and Provisioned |
| **Throttle rate** | % of requests that got rejected due to capacity limits |
| **EFO consumer count** | Number of registered Enhanced Fan-Out consumers |

**Important:** Average throughput is calculated over the **full 7-day window**, not just the hours with traffic. This prevents short bursts from looking like sustained throughput.

### Step 6: Per-stream recommendation

This produces per-stream actions (provisioned vs on-demand only — Advantage is handled separately as an account-level decision):

```
┌─────────────────────────────────────────────────────────────────────┐
│ Rule 1: Is the stream PROVISIONED and getting THROTTLED (>1%)?      │
│         → Switch to ON-DEMAND immediately              [HIGH prio]  │
├─────────────────────────────────────────────────────────────────────┤
│ Rule 2: Is traffic highly variable (peak > 3x average)?             │
│         → Use ON-DEMAND (auto-scales)                  [MED prio]   │
├─────────────────────────────────────────────────────────────────────┤
│ Rule 3: Is PROVISIONED mode >15% cheaper AND traffic is stable?     │
│         → Stay PROVISIONED (but note on-demand is simpler) [LOW]    │
├─────────────────────────────────────────────────────────────────────┤
│ Rule 4: None of the above?                                          │
│         → Default to ON-DEMAND (less ops)              [LOW prio]   │
└─────────────────────────────────────────────────────────────────────┘
```

**The bias:** On-demand is strongly preferred. Provisioned is only recommended when it's more than 15% cheaper AND the workload is stable.

### Step 7: Account-level On-demand Advantage assessment

On-demand Advantage is an **account-level setting per region** — you enable it for the entire account, not per stream. The agent assesses eligibility separately:

| Trigger | Threshold |
|---------|-----------|
| Aggregate sustained throughput across all streams | ≥ 10 MiB/s |
| EFO consumers on any single stream | > 2 |
| Total streams in the account/region | > 50 |

If any trigger is met, the report includes an "Enable On-demand Advantage" recommendation with the reasoning. The action is: first switch provisioned streams to on-demand, then enable Advantage at the account level.

### Step 8: Generate the report

Takes all the per-stream results and the account-level Advantage assessment and produces:

- **JSON** — for automation, dashboards, or downstream processing
- **HTML** — visual report with:
  - Per-stream actions (provisioned → on-demand where needed)
  - Account-level Advantage recommendation section (separate from per-stream)
  - Stats grid, priority indicators, and detailed reasoning

### Step 9: Store in S3

Both files go to S3 under:

```
s3://bucket/kds-optimization-reports/YYYY/MM/DD/HHMMSS-{report-id}.json
s3://bucket/kds-optimization-reports/YYYY/MM/DD/HHMMSS-{report-id}.html
```

## What makes it an "agent" vs just a Lambda?

The **Bedrock Agent** is the AI layer on top. It:

- Understands natural language ("analyze my streams")
- Decides which API to call (analyzeStreams, generateReport, or getStreamDetails)
- Interprets the results and writes a human-friendly summary
- Can answer follow-up questions ("why did you recommend on-demand for stream X?")

The Lambda does the heavy lifting (data collection + math). The agent makes it conversational and autonomous.
