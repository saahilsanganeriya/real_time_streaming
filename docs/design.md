# Real-Time Data Transformation Platform: design document

**Author:** Saahil Sanganeriya\
**Project:** Real-Time Data Transformation Platform using Amazon Managed Service for Apache Flink and OpenSearch\
**Timeline:** May 27, 2025 – August 15, 2025\
**Status:** complete

I wrote this design document for the pipeline in this repo during my summer 2025 internship. This copy is cleaned
for publication: names of people, links to internal tools and dashboards, and internal team and system names are
removed or described in plain words, and the diagrams are redrawn as text. Everything else, every number included,
is as the original states it. Links to the code now point at the same files in this repo. The challenges write-up
was a separate document and is published next to this one as [challenges.md](challenges.md).

## Contents

- [Context and problem](#context-and-problem)
- [Requirements](#requirements)
- [Architecture](#architecture)
- [Data model](#data-model)
- [Alternatives considered](#alternatives-considered)
- [Trade-offs](#trade-offs)
- [Failure handling](#failure-handling)
- [Operations and monitoring](#operations-and-monitoring)
- [Results](#results)
- [Challenges](#challenges)
- [Open questions and future work](#open-questions-and-future-work)
- [References](#references)

## Context and problem

### Introduction

This project moves the "Shift Opportunity Service Events" data pipeline from batch processing to a real-time
streaming architecture. The batch system processed business-critical shift opportunity events once an hour, which
delayed operational visibility and decisions. The new design uses Amazon Kinesis Data Streams, Amazon Managed Service
for Apache Flink and Amazon OpenSearch for real-time processing and visualization.

### Customers and business impact

The customers are internal business intelligence engineers, product managers and engineering teams who depend on
timely metrics and alerts to make decisions.

- **Current pain:** hourly batch processing delays operational visibility, which affects workforce planning and
  efficiency.
- **Technical challenges:** little flexibility for data enrichment and for complex nested JSON.
- **Value of the solution:** real-time insight through dashboards and alerts, so operations can respond as things
  happen and be managed ahead of problems.

### Goal and timeline

- **Primary goal:** build and deploy a production-ready real-time data transformation platform that processes shift
  opportunity events with low latency.
- **Timeline:** May 27 – August 15, 2025.
- **Deliverable:** a fully working, maintainable platform, ready for production handoff.

### Analysis of the existing system

- **Limitations:** no real-time alerting, and little operational visibility between batch processing windows.
- **Data characteristics:** deeply nested JSON with arrays, maps and timestamp fields, across three distinct event
  types that each need different processing logic.
- **Event-driven source:** the production system processes changes incrementally. It publishes discrete events for
  create, update and delete operations that carry the specific changes, not periodic database snapshots, so each
  change can be processed as soon as it happens.

### Glossary

- **SNS:** Amazon Simple Notification Service, a message publishing service.
- **Kinesis:** Amazon Kinesis Data Streams, a real-time data streaming service.
- **Flink:** Apache Flink, a distributed stream processing framework.
- **OpenSearch:** Amazon OpenSearch Service, a search and analytics engine.
- **DLQ:** dead-letter queue, where messages that failed processing are kept for handling.
- **VPC:** Virtual Private Cloud, an isolated network environment.

## Requirements

### Functional

1. **Event processing:** process shift opportunity events from a cross-region SNS topic and keep event ordering.
2. **Data transformation:** flatten nested JSON, extract metadata, handle missing fields, compute derived status
   fields and detect field changes.
3. **Storage:** a multi-sink pattern with a current-state view (upsert), a complete audit trail (append only) and
   denormalized views for analytics.
4. **Visualization:** real-time dashboards with filtering.

### Non-functional requirements and SLA

1. **Performance:** near real-time end-to-end latency (SNS → OpenSearch), with a target of under 120 seconds, and a
   throughput of thousands of events an hour.
2. **Reliability:** high availability with a recovery time under 5 minutes, automated failover and scaling, and
   exactly-once processing guarantees.
3. **Security:** handling that matches the data's internal classification, KMS encryption, VPC-restricted access and
   least-privilege IAM.
4. **Retention:** 7 days in Kinesis, long term in S3, configurable in OpenSearch.

## Architecture

### High-level design

```
 ap-southeast-2 (Sydney)       │ us-east-1 (N. Virginia)
                               │
 upstream scheduling service   │
 creates shift opportunities   │
        │                      │
        ▼                      │
 SNS topic                     │
 ShiftOpportunityServiceEvents │
        │                      │
        ├── SNS subscription ──┼──► Kinesis Firehose ──► S3: raw event data
        │                      │                               ┆ manual
        │                      │                               ┆ historical
        │                      │                               ┆ backfill
        └── SNS subscription ──┼──► Lambda processor ◄┄┄┄┄┄┄┄┄┄┄┘
              ┆ failure        │    reformats each event and acts
              ▼                │    as the bridge to Kinesis
 DLQ for cross-region SNS      │      │  ┆ failure, < 3 retries
 subscription failures         │      │  ├┄┄► DLQ for Lambda failures
        ┆ queue length ≥ 1     │      │  ┆       ┆ retry: replayed into the Lambda
        ▼                      │      │  ┆ failure, ≥ 3 retries
 alert for manual review       │      │  └┄┄► poison queue (permanent failures)
                               │      │         ┆ queue length ≥ 1
                               │      │         ▼
                               │      │       alert for manual review
                               │      ▼
                               │ Kinesis Data Stream
                               │      │
                               │      ▼
                               │ Apache Flink streaming application
                               │ (real-time transformations, always running)
                               │      ├──► S3: processed event data
                               │      └──► OpenSearch ──► OpenSearch Dashboards
                               │                          business operations,
                               │                          performance

 Across both regions: alerts, CloudWatch, KMS encryption, IAM permissions.
 Solid lines carry events. Dotted lines are failure, retry and backfill paths.
```

**Data flow overview.** The platform is event-driven, with error handling at each stage.

1. **Event generation and cross-region distribution.** Events start in business systems and are published to a
   cross-region SNS topic in ap-southeast-2. They are processed in us-east-1, the region that lines up with the
   team's existing downstream services and infrastructure.
2. **Dual-path ingestion.** When an event is published, it takes two parallel paths. Kinesis Firehose copies the raw
   event to S3 to preserve the data (see [data ingestion](#data-ingestion)), and a Lambda function processes it for
   immediate real-time streaming.
3. **Lambda processing and simplified error handling.** The Lambda processor routes each invocation to one of three
   processing modes by event type: real-time SNS-to-Kinesis processing, DLQ replay, and S3 historical replay for
   backfill. It reformats each SNS message for Flink (it wraps the message with its metadata and a Lambda timestamp),
   picks a partition key to spread the load, and publishes to Kinesis Data Streams. The Lambda is the bridge between
   SNS and Kinesis, because there is no native integration between the two services. Error handling, with DLQ
   management, lives in this one function (see [Lambda processor](#lambda-processor)).
4. **Apache Flink stream processing.** A Java Flink application reads the events from Kinesis and transforms them:
   it flattens the nested JSON, extracts metadata, computes derived fields and detects changes. Flink keeps state to
   hold the data consistent and supports exactly-once processing (see
   [stream processing with Flink](#stream-processing-with-flink)).
5. **Multi-sink OpenSearch storage.** Processed events go to three specialized indexes: **current state** (upsert,
   for operational queries), **events log** (append only, for performance checks and raw data analysis) and
   **associates** (denormalized, for analytics).
6. **Consumption and visualization.** Each storage pattern serves its own use case through OpenSearch Dashboards:
   real-time operations, compliance workflows and business intelligence (see [dashboards](#dashboards)).
7. **Backup and recovery.** An S3 copy of the processed data adds disaster recovery. It is an append-only audit log:
   Flink writes every processed event as files, with no upsert, because S3 is object storage.

### Exploratory work

- **Kinesis stream testing.** I built test infrastructure with a 1-shard stream and a Cloud9 environment. Finding:
  on-demand scaling is better than provisioned shards for variable workloads (it scales within 15 seconds).
- **Producer evaluation.** I compared the Python SDK, the Java KPL and a Lambda-based producer. The key insight: SNS
  cannot subscribe a Kinesis data stream directly, and Kinesis Firehose adds batching latency we did not want. The
  solution is a Lambda function that subscribes to SNS and forwards each event to Kinesis at once, which keeps the
  path real time.

### Data ingestion

The SNS → Firehose → S3 and SNS → Lambda paths, the upper part of the diagram above.

- A cross-region SNS topic, `ShiftOpportunityServiceEvents`, owned by the upstream scheduling service's team,
  publishes JSON events with nested metadata, headers and serialized payloads from ap-southeast-2.
- One SNS subscription feeds a Kinesis Firehose stream, which writes the raw data straight to an S3 bucket.
- A second SNS subscription invokes the Lambda. It tries the invocation 3 times with the message. If all 3 fail, it
  sends the message to a DLQ and raises an alert so someone can review it by hand.
- Processing happens in us-east-1, not ap-southeast-2, because most of the team's existing infrastructure is in
  us-east-1. It also gave a learning opportunity.
- The raw copy is not real time, because Firehose buffers data by size and time before delivery. It does mean no
  history is lost when something downstream fails, and it keeps data preservation apart from real-time processing.
  - Batch settings: 5 MB or a 60-second interval, with GZIP compression. GZIP was chosen over Snappy's faster
    decompression for about 70% lower storage cost. That suits historical data that is read rarely, and Spark and
    other analytics tools can process GZIP files in parallel when they are partitioned well.

### Lambda processor

One Lambda function routes between three processing modes based on the incoming event, which keeps operations simple
while still handling every kind of error.

**Processing mode detection and routing.** The processor works out its mode from the shape of the event:

- **SNS-to-Kinesis mode:** triggered by the SNS signature. Event source: SNS, or a simulated invocation with an
  SNS-like signature (see [development and testing](#development-and-testing-infrastructure)).
- **DLQ replay mode:** triggered by the SQS signature. Event source: SQS (the DLQ).
- **S3 historical replay mode:** a manual backfill event with the parameter `mode: "s3-historical-replay"`.

```
                              Lambda event
                                   │
                          event type detection
          ┌────────────────────────┼──────────────────────┐
          │ source: SNS            │ source: SQS          │ mode set to
          ▼                        ▼                      ▼ s3-historical-replay
 SNS_TO_KINESIS mode        DLQ_REPLAY mode       S3_HISTORICAL_REPLAY mode
          │                        │                      │
   send to Kinesis          check retry count     read S3 files and
     │          │             │            │      send them to Kinesis
  success    failure       < 3 retries  ≥ 3 retries       │
     │          │             │            │      more files, and the
     ▼          ▼             ▼            ▼      timeout approaching?
 publish     send to      retry sending  send to    │ yes          │ no
 success     the DLQ      to Kinesis     poison     ▼              ▼
 metrics        │          │        │    queue    reinvoke with  complete
                ▼       success  failure    │     the remaining
            warning        │        │       ▼     files
            alert          ▼        ▼    critical
                       delete    requeue with alert
                       from the  progressive delays
                       DLQ       (1m, 2m, 4m)
```

**Core processing logic.**

- **SNS processing:** reformats the raw SNS message into a structured envelope with added metadata, builds the
  partition key from the opportunity ID, and publishes to the Kinesis data stream.
- **DLQ processing:** automatic, event-driven retry with backoff and poison-queue handling.
- **S3 processing:** batch processing of historical data, with timeout management and continuation logic.

The retry rules, queues and alerts are in [failure handling](#failure-handling).

Code: [Lambda processor](../lambda-functions/unified-processor/),
[CloudFormation template for ap-southeast-2](../cloudformation-templates/sns-dlq-ap-southeast-2.yaml),
[CloudFormation template for us-east-1](../cloudformation-templates/unified-shift-opportunity-stack.yaml).

### Stream processing with Flink

Kinesis → Java Flink → OpenSearch, with several sinks.

```
 Kinesis source: ShiftOpportunityServiceEvents
        │
        ▼
 event deserialization: nested JSON parsing
        │
        ▼
 data quality filter: valid opportunity IDs
        │
        ├────────────────────────┬────────────────────────────┐
        ▼                        ▼                            ▼
 current state transform  audit log transform          associate expansion
 map, 1:1                 map, 1:1                     flatMap, 1:N
        │                  │             │                    │
        ▼                  ▼             ▼                    ▼
 current state index  events log index  S3 backup        associates index
 upsert               append            optional,        upsert + delete
        │                  │            long-term             │
        │                  │             │                    │
        ▼                  ▼             ▼                    ▼
 real-time dashboards compliance and   disaster         associate analytics
                      debugging        recovery
```

1. **Apache Flink application** (deployed as a Java JAR):
   1. **Event deserialization:** custom JSON parsing for the complex nested structures, with null safety.
   2. **Data quality filtering:** checks opportunity IDs and the essential fields before processing.
   3. **Multi-transform pipeline:**
      - **Current state transform:** 1:1 map, with business logic for Created, Updated and Deleted events. The
        upsert pattern keeps the latest state of each opportunity for operational dashboards.
      - **Audit log transform:** 1:1 map that keeps the full event history with change tracking. The append-only
        pattern keeps a complete audit trail for compliance.
      - **Associate expansion:** 1:N flatMap that creates one record per accepted associate. The denormalized
        pattern creates composite-key records (`opportunity_id#associate_id`) for workforce analytics.
2. **Custom OpenSearch sink:**
   1. **AWS IAM integration:** Signature Version 4 authentication, in place of the basic-auth limit of the standard
      connectors.
   2. **Conditional updates:** Painless scripts stop out-of-order events from making the data go backwards.
      1. The script compares the source timestamp of the incoming event with that of the document with the same ID
         in the index. If the incoming timestamp is newer or the same, the document is upserted. Otherwise the
         event is ignored.
      2. Only the current state index and the associates index use the conditional update. The events log gets
         every event, whatever its order.
   3. **Lifecycle management:** Java-based delete support to clean up associate records.

Code: [Flink Java application](../shift-opportunity-consumer-amazon-managed-service-for-apache-flink/).

### Managed Flink configuration

- **Runtime:** Apache Flink 1.19.1.
- **Parallelism:** auto-scaling between 1 and 4 KPUs (Kinesis Processing Units).
- **KPU size:** each KPU gives 1 vCPU and 4 GB of memory.
- **Scaling policy:** target 70% CPU use, with scale-up and scale-down rules.
  - **Scale up:** when CPU is above 75% for 15 minutes in a row; doubles the parallelism and KPUs.
  - **Scale down:** when CPU is below 10% for 6 hours in a row; halves the parallelism and KPUs. The slow,
    conservative rule prevents oscillation.
  - **Grace period:** at least 1 hour between a scale-up and any scale-down.
  - **Observed:** no scaling events, because traffic was low (1 event a minute, far below the thresholds).
- **Checkpointing interval:** every 10 seconds, for exactly-once processing.
- **Checkpoint mode:** `EXACTLY_ONCE`, with distributed snapshots.
- **State backend:** managed state with automatic recovery.
- **Checkpoint timeout:** 60 seconds, with automatic retry.
- **Event-time processing:** uses the business event timestamps, with a 5-second watermark tolerance.
- **Late events:** handled to balance completeness against latency.
- **Backpressure:** automatic flow control keeps memory from overflowing in high-volume periods.

> **Note on this copy:** the job in this repo builds against Flink 1.20.0 (see
> [`pom.xml`](../shift-opportunity-consumer-amazon-managed-service-for-apache-flink/pom.xml)). The runtime above is
> the one the document lists.

### Development and testing infrastructure

The platform includes an event simulator for realistic testing and validation.

```
 EventBridge schedule
        │
        ▼
 DataSimulator Lambda
        │
        ▼
 choose a random opportunity ID, 1…20
        │
        ▼
 query its state in DynamoDB
        │
 does this opportunity exist?
   │ no                           │ yes
   ▼                              ▼
 generate OpportunityCreated    generate update or delete events
   │                              │
   ▼                              ▼
 store the new state in         update the state in DynamoDB
 DynamoDB                         │
   └──────────────┬───────────────┘
                  ▼
 invoke the processing Lambda with a simulated SNS message
                  │
                  ▼
 real-time streaming platform
```

**Simulator architecture.**

- **DynamoDB state management:** tracks each opportunity's life cycle across create, update and delete.
- **Realistic data generation:** several countries (US, Mexico, Australia) with matching location mapping.
- **Production-like events:** realistic opportunity types (COVER, VET, VTO) and workgroups (ICQA,
  SORT_CENTER_GENERAL).

**DataSimulator Lambda function.**

- **State-aware processing:** keeps opportunity states in DynamoDB, with TTL management.
- **Configurable intervals:** scheduled events at an adjustable frequency (1–60 minutes).
- **Realistic patterns:** generates real business patterns, including claims, unclaims and modifications.

**Testing capabilities.**

- **Load testing:** configurable event rates for performance validation.
- **State transitions:** realistic opportunity life cycles with correct state management.

Code: [CloudFormation template](../cloudformation-templates/ShiftOppSimulator-stack.yaml),
[DataSimulator Lambda](../lambda-functions/ShiftOppSimulator-stack-DataSimulator.py).

### Security and compliance

- **Data classification:** the events hold shift schedules, location IDs and associate IDs, with no sensitive
  customer or financial information, and are handled under the matching internal data classification. They carry
  operational metadata, with 7 days of streaming retention, long-term storage in S3 and a complete audit trail.
- **Encryption strategy:** KMS-managed keys for Kinesis. S3 buckets use server-side encryption with Amazon S3
  managed keys.
- **Encryption:** AWS KMS managed encryption for all services, with automatic key rotation, and secure cross-region
  transfer with IAM authentication.
- **Network:** every service is restricted to VPC endpoints where they apply, with no internet gateway.
- **IAM:** scoped policies that follow least privilege, with dedicated roles for Lambda, Kinesis, Flink and
  OpenSearch.

## Data model

### Event types

The system processes three kinds of shift opportunity event. Each has its own data pattern and processing needs.

| Event | Data pattern | Use case | Key characteristic |
|---|---|---|---|
| OpportunityCreated | only `newOpp`, with the complete initial state | new shift opportunities published to the system | `oldOpp` is null or absent, so state starts clean |
| OpportunityUpdated | both `oldOpp` and `newOpp`, for change tracking | claims, modifications, status changes, associate assignments | allows change detection and the audit trail |
| OpportunityDeleted | only `oldOpp`, with the final state before deletion | opportunity cancellations and removals | `newOpp` is null or absent; triggers cleanup in the upsert indexes |

### Nested JSON

- Nesting goes **5 levels deep**, with arrays, maps and optional fields that differ by event type.
- **Several timestamp formats** must be standardized to ISO-8601, with time zone handling.
- **Which fields are present** depends on the event type (the Created, Updated and Deleted patterns above).

**Field extraction and computed fields.** Extraction handles nullable data by taking each field from `newOpp` first
and from `oldOpp` when `newOpp` lacks it. The job computes the fill status (OPEN, PARTIALLY_FILLED, FILLED), the
claim status (NOT_STARTED, ACTIVE, EXPIRED), change detection flags and latency metrics. These computed fields drive
the real-time business views and the platform performance views.

### Physical model

The payload as it arrives from the Lambda, and the shapes the Flink job makes from it. Every field, with its type and
description, is in [`physicalModel.mmd`](../shift-opportunity-consumer-amazon-managed-service-for-apache-flink/physicalModel.mmd)
(a Mermaid ER diagram).

```
 JSON payload (the Lambda's envelope)
 ├─ metadata
 │   ├─ messageId, sns_timestamp, lambda_timestamp, topicArn, subject
 │   └─ attributes: EventType, FieldsChanged, Country, OpportunityType
 └─ message
     ├─ entityId, eventTime, sequenceNumber
     └─ event
         ├─ serializationKey
         └─ serializedData
             ├─ oldOpp   previous state; absent for OpportunityCreated
             └─ newOpp   current state; absent for OpportunityDeleted
                 ├─ id, locationId, country, workgroup, source, type
                 ├─ timezone, timeRange: start, end
                 ├─ claimStartsAt, claimEndsAt
                 ├─ createdAt, createdBy, lastUpdatedAt, lastUpdatedBy
                 ├─ lastActionTaken, batchId
                 ├─ totalHeadCount, acceptedAssociateIds
                 ├─ dropPenaltyStartsAt, dropEndsAt
                 └─ shiftMatchRequestDetails, payModifiers
        │
        │ Java deserialization: each field COALESCE(newOpp, oldOpp)
        ▼
 ShiftOpportunityEvent   temporary processing view, plus computed fields
        │
        ├─ map, 1:1 ────► shift_opportunity_current      one per opportunity
        ├─ map, 1:1 ────► shift_opportunity_events_log   one per event
        └─ flatMap, 1:N ► shift_opportunity_associates   one per associate
```

The three OpenSearch indexes:

| Index | Document ID | Write pattern | Fields beyond the shared set |
|---|---|---|---|
| `shift_opportunity_current` | `opportunity_id` | upsert, conditional on `src_timestamp` | `accepted_associate_ids` |
| `shift_opportunity_events_log` | none: one document per event | append only | `message_id`, `entity_id`, `serialization_key`, `sequence_number`, `topic_arn`, `fields_changed`, `accepted_associate_ids`, five `previous_*` fields, `head_count_changed`, `accepted_associates_changed` |
| `shift_opportunity_associates` | `document_id`, which is `opportunity_id#associate_id` | upsert and delete, conditional on `src_timestamp` | `associate_id`, `is_deleted`, `lambda_timestamp`; `total_head_count` is a double here, for analytics |

The shared set, in all three indexes: the opportunity ID, event type, country, opportunity type, location, workgroup,
source, time zone, shift and claim windows, creation time and creator, last modifier and last action, head count,
batch ID, drop penalty window, fill and claim status, the source, SNS and Flink timestamps, and two latency metrics,
`end_to_end_latency_ms` and `sns_to_flink_latency_ms`.

## Alternatives considered

### Technology evaluation

Amazon Kinesis was chosen for its native AWS integration and managed scaling. Amazon Managed Service for Apache Flink
was chosen for advanced stream processing with the Java DataStream API and exactly-once semantics. Amazon OpenSearch
was chosen for its rich queries and built-in visualization, with support for several indexes.

### Processing approach comparison

| Data flow | Transformation | Processing guarantees | Business logic | Multi-sink | Latency | Scalability | Event-time processing | Integration | Cost (monthly) | Key limitation |
|---|---|---|---|---|---|---|---|---|---|---|
| Option 1: SNS → Lambda → Kinesis → OpenSearch Ingestion → OpenSearch | limited (complex nested JSON not supported) | no exactly-once guarantees | inflexible for computed fields | no multi-sink patterns | moderate | limited | no | good with OpenSearch | $178–207 | limits on complex JSON processing |
| Option 2: SNS → Lambda → Kinesis → Data Prepper → OpenSearch | batch-oriented, limited real time | no exactly-once guarantees | not enough for 1:N transformations | limited | high (batch delays) | moderate | no watermark support | limited Kinesis integration | $123–155 | batch processing conflicts with the real-time requirement |
| Option 3: SNS → Lambda → OpenSearch directly | must be built by hand | no exactly-once (multiple invocations) | limited stateful processing | built by hand | good | limited parallelism | watermarks by hand | moderate | $38–53 | no exactly-once semantics or stateful processing |
| Option 4: SNS → Firehose → S3 → Lambda → OpenSearch | limited compared with full programming | no exactly-once guarantees | no real-time computed fields | no | poor (1–15 minutes) | moderate | no | good with AWS services | $28–38 | built-in batching adds unacceptable delay |
| **Option 5: SNS → Lambda → Kinesis → Flink → OpenSearch (chosen)** | full Java programming model | exactly-once semantics | complex business logic | native multi-sink patterns | sub-second | horizontal auto-scaling | advanced watermarks | excellent with Kinesis and OpenSearch | $188–227 | none; meets every requirement |

**Monthly cost calculations** (development environment, about 50 KB of data a day):

- **Option 1:** Kinesis stream $29 + 7-day retention $0.15 + 1 OCU of OpenSearch Ingestion $175 + OpenSearch
  Serverless storage ~$3 = **$178–207**
- **Option 2:** Kinesis stream $29 + 7-day retention $0.15 + EC2 for Data Prepper (t3.medium) $31 + OpenSearch dev
  cluster ~$75 = **$123–155**
- **Option 3:** Lambda (~150 invocations) $0.03 + OpenSearch dev cluster ~$75 = **$38–53** (the cheapest, but with
  limited function)
- **Option 4:** Firehose $0.04 + S3 storage $0.03 + Lambda processing $3 + OpenSearch dev cluster ~$75 = **$28–38**
- **Option 5:** Kinesis stream $29 + 7-day retention $0.15 + Flink (1 KPU) $80 + OpenSearch dev cluster ~$75 =
  **$188–227** (the most expensive, but it meets every requirement)

Estimates use AWS pricing for us-east-1 in August 2025, for a development-scale deployment that processes about
50 KB a day with small clusters. More detail is in [cost analysis](#cost-analysis).

### Firehose with zero buffering

How Firehose with zero buffering compares with the Flink platform, and what that means for the decision:

| Capability | Firehose (zero buffering) | Flink platform | Assessment |
|---|---|---|---|
| Latency | ~5–10 seconds | < 1 second | Firehose improved, but still higher |
| Stateful processing | no keyed state or watermarks | full event-time semantics | critical gap remains |
| Multi-sink coordination | a single destination only | 3 indexes with different patterns | architecture requirement |
| Exactly-once effects | at-least-once delivery | idempotent conditional updates | data consistency requirement |
| Complex transformations | limited to Lambda transforms | associate expansion, change detection | business logic requirement |
| OpenSearch integration | basic bulk insert only | conditional upserts and deletes with Painless | advanced feature requirement |

**Verdict:** zero buffering improves Firehose's latency a great deal, but it is still not enough for this use case,
which needs stateful processing, coordination across sinks and conditional OpenSearch operations. The Flink design
is the only option that meets every technical requirement.

**Possible future use:** zero-buffering Firehose suits simple monitoring or backup streams, where latency of a few
seconds is fine and no complex transformation is needed.

### Java DataStream API

Chosen over the Python Table API, Flink SQL and the Scala DataStream API.

1. **Feature completeness:** the DataStream API gives full access to Flink's advanced features, including complex
   stateful operations.
2. **Complex business logic:** needed for the associate expansion and for change detection.
3. **Performance:** better throughput and memory management for high-volume event processing.
4. **Custom sink:** AWS IAM authentication for OpenSearch needed a custom sink.
5. **Production maturity:** many enterprise deployment patterns and full debugging tools.
6. **Team experience:** the team had little Scala experience.

**Python problems met.** Development started with the Python Table API, which hit critical limits after 3+ weeks of
work:

- **Version compatibility:** PyFlink, Flink and the connector versions had conflicting dependencies.
- **Authentication:** the Python Flink connectors had no AWS IAM authentication.
- **Connectors:** many enterprise connectors were missing or unstable.
- **Debugging:** stack traces and error messages said less.
- **Table API limits:** not flexible enough for complex business logic.

These limits forced the move to the Java DataStream API, which gave full AWS SDK integration, custom sinks and the
reliability a production deployment needs. The full story is in [challenges.md](challenges.md).

**Scala assessment.**

Benefits:

- **Functional programming:** shorter, more expressive code for stream transformations.
- **Type safety:** strong compile-time guarantees for complex data processing pipelines.
- **Java interoperability:** works directly with Flink's Java APIs and ecosystem.
- **Flink 1.20+ support:** full compatibility with Scala 3.6.2 and current Flink versions, with good connector
  support.

Trade-offs:

- **Learning curve:** harder to adopt at first than Java.
- **Debugging:** more complex stack traces and debugging in distributed environments.
- **Dependency management:** more work to keep Scala versions compatible with Flink.

**Decision:** Scala's functional style fits stream processing well, but Java knowledge was more widespread, which made
Java the practical choice for fast development and confidence in operations.
[Historical integration examples](https://themodernlife.github.io/scala/hadoop/hdfs/sclading/flink/streaming/realtime/2015/12/20/running-scalding-jobs-on-apache-flink/)
show that Scala works for complex Flink applications, which makes it a strong candidate for future projects once the
team's skills grow.

### Custom OpenSearch sink

The standard Flink OpenSearch connector has critical limits for production use:

- no AWS IAM authentication (basic auth only);
- no conditional updates to protect against out-of-order events;
- limited delete support for complex data life cycles.

The custom sink provides:

- **AWS Signature Version 4:** secure access to Amazon OpenSearch Service with no stored credentials.
- **Conditional updates:** Painless scripts stop out-of-order events from making the data go backwards.
- **Delete support:** Java-based life cycle management for the normalized associate data.

## Trade-offs

The main trade-offs, each made in the section linked:

| Decision | What it gains | What it costs |
|---|---|---|
| Flink over the cheaper designs ([comparison](#processing-approach-comparison)) | sub-second latency, exactly-once, several sinks from one stream | the highest monthly cost, $188–227 |
| One Lambda invocation per SNS record ([scaling](#sns-lambda-scaling-and-micro-batching)) | the best latency, fine-grained control | higher Lambda cost at extreme scale, and the concurrency limit |
| A separate Firehose path for raw events ([ingestion](#data-ingestion)) | no history lost when something downstream fails | that copy is not real time |
| GZIP for the raw archive ([ingestion](#data-ingestion)) | about 70% lower storage cost | slower decompression than Snappy |
| Kinesis on-demand ([cost analysis](#cost-analysis)) | no shard management, 60–80% savings at low traffic | ~2.5x the price of provisioned capacity |
| Conservative scale-down ([configuration](#managed-flink-configuration)) | no oscillation | 6 hours below 10% CPU before capacity halves |
| Conditional and fast sink modes ([challenges](challenges.md#3-custom-opensearch-sink-development-and-painless-scripts)) | a choice per index between consistency and speed | the fast modes skip the timestamp check |
| Java over Scala ([Java DataStream API](#java-datastream-api)) | the team's wider Java knowledge | less concise code |
| Flushing after each action ([future work](#high-volume-performance)) | immediate writes to OpenSearch | more requests at high volume |

### SNS-Lambda scaling and micro-batching

**Current architecture:** each SNS record triggers one Lambda invocation, for real-time performance (sub-200 ms
processing per event).

**Scaling analysis:**

- **Lambda concurrency limit:** 1,000 concurrent executions (the default).
- **Invocation duration:** ~200 ms per event.
- **Theoretical capacity:** ~300,000 events a minute before the concurrency limit.
- **Current load:** ~50 KB a day (~70 events a day), far within the limits.

**Options for high volume:**

| Approach | Implementation | Benefits | Trade-offs | When to use |
|---|---|---|---|---|
| SQS micro-batching | SNS → SQS → Lambda (batch processing) | fewer Lambda invocations, lower cost | slightly more latency (seconds) | > 100,000 events a minute |
| Firehose zero buffering | SNS → Firehose → OpenSearch | managed scaling, no Lambda concurrency limit | limited transformations | simple pass-through cases |
| Current 1:1 processing | SNS → Lambda → Kinesis | the best latency, fine-grained control | higher Lambda cost at extreme scale; the concurrency limit blocks | < 300,000 events a minute |

**Recommendation:** keep the 1:1 SNS-Lambda design for the real-time performance requirement. Consider SQS
micro-batching only if volume goes above 1 million events a minute, where the small latency cost becomes worth the
savings.

### Cost analysis

The cost structure balances performance, reliability and operational efficiency while scaling predictably with
sub-second latency. All prices are for us-east-1, August 2025.

| Service | Item | Unit | Price | Notes |
|---|---|---|---|---|
| Kinesis Data Streams | on-demand, per stream | stream-hour | $0.04 | auto-scales; ~2.5x premium over provisioned |
| | on-demand data ingestion | GB | $0.08 | includes 24-hour retention |
| | extended retention (7 days) | GB-month | $0.10 | extra cost for 7-day retention |
| | provisioned | shard-hour | $0.02 | 1 MB/s write, 2 MB/s read capacity |
| Managed Flink | KPU (compute) | KPU-hour | $0.11 | 1 vCPU, 4 GB memory, ~1 MB/s throughput |
| | application storage | GB-month | $0.10 | 50 GB included per KPU |
| | orchestration overhead | KPU-hour | $0.11 | one extra KPU per application |
| Lambda | requests | million requests | $0.20 | first million each month free |
| | duration (ARM) | GB-second | $0 | 20% savings over x86 |
| OpenSearch | t3.small.search | instance-hour | $0.04 | development workloads |
| | m6g.large.search | instance-hour | $0.13 | balanced production |
| | GP3 storage | GB-month | $0.14 | 3,000 IOPS baseline |
| | Serverless OCUs | OCU-hour | $0.24 | minimum 2 OCUs for high availability |
| Supporting services | S3 Standard | GB-month | $0.02 | primary data storage |
| | SNS publish | million requests | $0.50 | cross-region notifications |
| | SQS DLQ | million requests | $0.40 | error handling queues |

**Kinesis on-demand benefits:**

- **Cost:** 60–80% savings in low-traffic periods, through automatic scaling to zero.
- **Operations:** no shard management or capacity planning.
- **Bursts:** scales to 2x peak use within 15 seconds.

On-demand is cost-effective for traffic that varies a lot.

**7-day retention.** Raising retention from the default 24 hours to 7 days (~$0.15 a month extra) gives:

- replay for debugging and data quality issues;
- recovery of historical data during downstream outages;
- room for testing and validation during development;
- audit trail preservation for compliance.

**DLQ economics.** For a small cost (~$2–5 a month), the DLQs prevent data loss incidents, make debugging
systematic, and cut incident response time from hours to minutes.

## Failure handling

### Retries, dead letters and the poison queue

All error types go through one error handling system that is easy to manage:

- **DLQ:** a single queue for every error type (SNS parsing, Kinesis failures, validation errors), with 14-day
  retention.
- **Event-driven processing:** an SQS event source mapping triggers DLQ processing automatically, with no scheduled
  jobs.
- **Retry logic:** progressive exponential backoff (60 s → 120 s → 240 s), at most 3 retry attempts.
- **Poison queue:** the permanent destination for messages that pass the retry limit, with a critical alert.
- **Real-time alerting:** SNS notifications at every error stage, with a severity level (WARNING or CRITICAL). See
  [monitoring and alerting](#monitoring-and-alerting).
- **Cross-region DLQ:** a separate DLQ on the SNS subscription in ap-southeast-2, for cross-region delivery failures.
- **SNS delivery:** the subscription tries to invoke the Lambda 3 times before a message goes to that DLQ.

The [Lambda flowchart](#lambda-processor) shows how each mode moves a message between these queues.

### Out-of-order and duplicate events

- The current state and associates indexes take an update only when its source timestamp is newer than, or the same
  as, the stored document's. A late event is ignored without an error, so the data never goes backwards, and a
  repeated event writes the same document again. These idempotent conditional updates give exactly-once effects in
  the two indexes.
- The events log takes every event, whatever its order, so it is the complete history.
- Flink checkpoints every 10 seconds in `EXACTLY_ONCE` mode and works in event time, with a 5-second watermark
  tolerance for late events (see [configuration](#managed-flink-configuration)).

### Replay and recovery

- **DLQ replay:** the same Lambda reads the DLQ through the SQS event source mapping and resends to Kinesis.
- **S3 historical replay:** a manual backfill reads the raw events that Firehose archived in S3 and sends them to
  Kinesis. It reinvokes itself with the remaining files when its timeout approaches.
- **Kinesis retention:** 7 days of retention allows replay for debugging and recovery during downstream outages.
- **Flink state:** managed state and checkpoints give automatic recovery, against the recovery-time target of under
  5 minutes.
- **Processed copy:** the S3 backup of processed events gives disaster recovery.

## Operations and monitoring

### Monitoring and alerting

The platform is monitored with Amazon CloudWatch. Real-time alerting sends an email at once for every error
condition, in both regions, and a cross-region CloudWatch dashboard collects the metrics.

**Alert levels:**

- **WARNING:** new DLQ messages, processing retries, performance degradation.
- **CRITICAL:** poison-queue messages, AWS connectivity failures, system errors.

**Alert coverage:**

| Alert | Threshold | Period | Severity | Description |
|---|---|---|---|---|
| Lambda error rate | ≥ 5 errors | 5 minutes | WARNING | Lambda error detection |
| Poison queue messages | ≥ 1 message | 1 minute | CRITICAL | permanent failures that need investigation |
| Processing success rate | < 95% | 5 minutes | WARNING | processing quality degradation |
| SNS DLQ messages (ap-southeast-2) | ≥ 1 message | 5 minutes | WARNING | cross-region delivery problems |
| High processing duration | > 4 minutes average | 10 minutes | WARNING | performance degradation |

> **Note on this copy:** the table is the alerting design. The CloudFormation templates in this repo define four
> alarms in the us-east-1 stack (Lambda error rate, the Lambda DLQ, the poison queue, success rate) and the SNS DLQ
> alarm in the ap-southeast-2 stack.

### Dashboards

OpenSearch Dashboards give real-time views of both business operations and system performance. They refresh from the
three OpenSearch indexes. The saved dashboards are in [`opensearch dashboards/`](../opensearch%20dashboards/).

**Business operations dashboard**, for operations teams and business stakeholders:

- **Regional performance:** opportunity volume and success rates by country.
- **Opportunity type performance:** business impact by type (COVER, VET, VTO), with head count metrics.
- **Workgroup efficiency:** use patterns across workgroups (ICQA, SORT_CENTER_GENERAL and others).
- **Top active associates:** the most engaged associates, ranked.
- **Geographic activity:** associate participation by location and country.

**Performance and system health dashboard**, for platform operations and performance tuning:

- **System latency:** end-to-end and SNS-to-Flink latency against sub-second targets.
- **Throughput and volume:** event processing rates and volume by event type.
- **Event volume over time:** the trend of the processing load.
- **Performance by location:** latency and processing metrics per location.
- **Recent events table:** single processed events, for debugging.

The two dashboards use the three-index design to give both business views and detailed technical metrics.

## Results

**Volume.** The platform was built in a gamma test environment with a testing volume under 50 KB a day. Production
volume is expected to be higher.

**Performance:**

- **Target and result:** the target was under 120 seconds; the platform achieved 600–900 ms processing latency.
- **End to end:** 5–10 seconds of upstream delay + 600–900 ms in the platform.
- **By component:** SNS → Lambda → Kinesis < 500 ms, Kinesis → Flink 200–400 ms, Flink → OpenSearch < 100 ms.

The platform consistently processes events in under a second, from SNS receipt to OpenSearch indexing. The main
source of latency is the delay in the upstream system, which the platform cannot control.

**Headroom.** At ~200 ms per invocation and the default 1,000 concurrent executions, the Lambda stage can take about
300,000 events a minute ([scaling analysis](#sns-lambda-scaling-and-micro-batching)). That is a calculation, not
measured traffic.

## Challenges

Building the platform met several hard technical problems, each solved. The [challenges write-up](challenges.md)
covers each one, with its impact, the solution and the lessons:

1. OpenSearch VPC deployment complexity
2. Python Flink development challenges (Studio notebooks versus a streaming application)
3. Custom OpenSearch sink development and Painless scripts
4. Authentication architecture decisions
5. Cross-region architecture complexity
6. Real-time versus batch processing trade-offs

## Open questions and future work

### Production implementation

- Deploy the platform to production, with capacity planning and monitoring.
- Set production SLAs and operational runbooks for the real-time pipeline.

### Schema evolution

- **Current limit:** the application expects a fixed schema. Adding or removing fields breaks it.
- **Improvement:** handle schema evolution, with try-catch handling or dynamic field mapping.
- **Benefit:** backward and forward compatibility for changing data structures, with no redeployment.

### Wider error handling

- **Current gap:** the SNS-to-Firehose subscription has no DLQ for large-scale outages and shutdowns.
- **Improvement:** add a DLQ to the Firehose subscription for delivery failures during outages.
- **Benefit:** error handling on every data path, for a zero-data-loss guarantee.

### High-volume performance

- **Current configuration:** the OpenSearch sinks use `.setBulkFlushMaxActions(1)`, which flushes after every action.
- **Improvement:** for higher volumes, raise the bulk flush size (for example, 100–1000 actions per batch).
- **Trade-off:** slightly more latency for fewer requests and better throughput.
- **Benefit:** Flink can handle much higher volumes with less OpenSearch API request overhead.

### Choices to revisit

- Zero-buffering Firehose for simple monitoring or backup streams ([Firehose with zero buffering](#firehose-with-zero-buffering)).
- SQS micro-batching if volume goes above 1 million events a minute
  ([scaling](#sns-lambda-scaling-and-micro-batching)).
- Scala for future projects, once the team's skills grow ([Java DataStream API](#java-datastream-api)).

## References

**This repo:**

- [Challenges faced and solutions](challenges.md)
- [Physical data model](../shift-opportunity-consumer-amazon-managed-service-for-apache-flink/physicalModel.mmd)
- [Flink application](../shift-opportunity-consumer-amazon-managed-service-for-apache-flink/) and its
  [technical architecture notes](../shift-opportunity-consumer-amazon-managed-service-for-apache-flink/TECHNICAL_ARCHITECTURE.md)
- [Lambda processor](../lambda-functions/unified-processor/)
- [CloudFormation templates](../cloudformation-templates/)

The design went through two midpoint reviews before this final version: one of the design document with the
high-level design, and one of the low-level design.

**AWS service documentation:**

- [Amazon Kinesis Data Streams](https://docs.aws.amazon.com/kinesis/latest/dev/)
- [Amazon Managed Service for Apache Flink](https://docs.aws.amazon.com/managed-flink/latest/java/what-is.html)
- [Amazon OpenSearch Service](https://docs.aws.amazon.com/opensearch-service/)
- [AWS Lambda](https://docs.aws.amazon.com/lambda/latest/dg/)
