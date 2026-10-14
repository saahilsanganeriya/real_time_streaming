# Real-time streaming pipeline

A shift opens up at a warehouse, and somebody needs to know. In the old setup the dashboard that should show it
caught up once an hour, when a batch job ran. This repo is the streaming pipeline I built to replace that job, during
my summer 2025 data engineering internship at Amazon. With it, an event is searchable less than a second after it
reaches the pipeline.

It reads shift-opportunity events from an SNS topic in the Sydney region, carries them to a Kinesis stream in
N. Virginia, transforms them in an Apache Flink job, and writes three views of the data to OpenSearch.

## Result

- **600–900 ms end to end**, from SNS to a searchable OpenSearch document. The target was 120 seconds.
- The upstream service took another 5–10 seconds to publish each event. Nothing in this pipeline can change that part.
- The Lambda stage has headroom for 300K+ events a minute. That is a capacity estimate, not traffic it carried.
- Other teams later reused the design for their own real-time work.

## How it works

```
 upstream service ── SNS topic (Sydney)
                          │  cross-region subscription
                          ▼
                 Lambda processor (N. Virginia) ──── fails ───► dead-letter queue
                 validates, enriches, retries                         │
                          │                                           │ replay
                          │ ◄─────────────────────────────────────────┘
                          │ ◄──── replay ──── S3 archive of raw events (filled outside this repo)
                          ▼
                 Kinesis Data Stream
                          │
                          ▼
                 Flink job (Java DataStream API)
                    ├──► OpenSearch: current state   upsert, older updates ignored
                    ├──► OpenSearch: event log       append only
                    ├──► OpenSearch: per associate   one document per assigned worker, upsert and delete
                    └──► S3 (optional copy of the log)
                          │
                          ▼
                 OpenSearch Dashboards
```

Each event is deeply nested JSON. The Flink job flattens it, works out a status, notes which fields changed, and
sends it to three indexes that answer different questions:

| Index | Write pattern | Question it answers |
|---|---|---|
| current state | upsert, one document per opportunity | What does this shift look like right now? |
| event log | append only | What happened, and in what order? |
| per associate | upsert and delete, one document per worker | Who is on which shift? |

Events can arrive out of order. By default each update goes to OpenSearch with a short Painless script that compares
timestamps there and keeps the stored document if it is newer. That gives exactly-once results without a read before
every write. A faster mode skips the check for streams whose order is guaranteed. Counting both modes, the job wires
five sink variants, and in production each one signs its requests with IAM (SigV4).

Failed records go to a dead-letter queue. The same Lambda can replay that queue into Kinesis, or re-read archived raw
events from S3. Four CloudWatch alarms watch the error rate, both failure queues and the success rate.

## Why Flink

I compared five designs before writing the job:

| Design | Latency | Exactly-once | Several sinks from one stream |
|---|---|---|---|
| SNS → Firehose → S3 → Lambda → OpenSearch | minutes of buffering | no | no |
| SNS → Lambda → OpenSearch directly | good | no | by hand |
| SNS → Lambda → Kinesis → Data Prepper → OpenSearch | batch delays | no | limited |
| SNS → Lambda → Kinesis → OpenSearch Ingestion → OpenSearch | moderate | no | no |
| **SNS → Lambda → Kinesis → Flink → OpenSearch** | **under a second** | **yes** | **yes** |

Flink cost the most at development scale. It was also the only design that could keep updates in order and write
three differently shaped indexes from one stream.

## What went wrong

I started in a Flink Studio notebook with PyFlink and SQL. One sink worked. The second one hung: the job looped on
the first `INSERT` and never reached the next, and the docs confirmed that notebooks allow a single sink. I got
several sinks running by editing the generated code so that every `INSERT` ran in one statement set.

Then I hit two limits I could not edit around. The Python Table API had no way to make an update conditional on a
timestamp, and it only supported username and password auth, not IAM. I threw away more than three weeks of notebook
work and rewrote the job in Java. The lesson I kept: check the auth and sink requirements against the runtime before
building the prototype, not after.

## What is in the repo

| Path | What it is |
|---|---|
| [`shift-opportunity-consumer-amazon-managed-service-for-apache-flink/`](./shift-opportunity-consumer-amazon-managed-service-for-apache-flink/) | the Flink job (Java 11, Flink 1.20) with its own README and architecture notes |
| [`lambda-functions/unified-processor/`](./lambda-functions/unified-processor/) | the Lambda that moves SNS events into Kinesis, plus the dead-letter and S3 replay modes |
| [`lambda-functions/ShiftOppSimulator-stack-DataSimulator.py`](./lambda-functions/ShiftOppSimulator-stack-DataSimulator.py) | a simulator that generates fake events for testing |
| [`cloudformation-templates/`](./cloudformation-templates/) | stacks for the processor, the cross-region subscription and its dead-letter queue, and the simulator |
| [`iam-roles/`](./iam-roles/) | an export of the Flink application role |
| [`opensearch dashboards/`](./opensearch%20dashboards/) | saved dashboards for the business view and system health |
| [`docs/`](./docs/) | the [design document](./docs/design.md) I wrote for the project, and its [challenges write-up](./docs/challenges.md) |

## Running it

This was built for a company AWS environment. It is here to read, and it will not run as it stands: you need your
own SNS topic, Kinesis stream and OpenSearch domain. ARNs in the docs and test files use `arn:<partition>:` and the
example account `123456789012` as placeholders. The CloudFormation stacks that subscribe to the upstream topic take it
as a required parameter.

To build the Flink job:

```sh
cd shift-opportunity-consumer-amazon-managed-service-for-apache-flink
mvn package
```

The [Flink README](./shift-opportunity-consumer-amazon-managed-service-for-apache-flink/README.md) and the
[processor README](./lambda-functions/unified-processor/README.md) cover configuration.
