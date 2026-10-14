# Real-Time Data Transformation Platform: challenges faced and solutions

**Author:** Saahil Sanganeriya\
**Project:** Real-Time Streaming Data Platform using Amazon Managed Service for Apache Flink and OpenSearch\
**Document:** technical challenges and solutions, an appendix to the [design document](design.md)

This write-up was a separate appendix to the design document. Like that document, this copy is cleaned for
publication: internal tool names are described in plain words. Everything else, every number included, is as the
original states it.

## Overview

Building this real-time streaming platform met several hard technical problems, and each one was solved. This
document covers each problem, the solution and the lessons.

- [1. OpenSearch VPC deployment complexity](#1-opensearch-vpc-deployment-complexity)
- [2. Managed Flink Studio notebook versus streaming application](#2-managed-flink-studio-notebook-versus-streaming-application)
- [3. Custom OpenSearch sink development and Painless scripts](#3-custom-opensearch-sink-development-and-painless-scripts)
- [4. Authentication architecture decisions](#4-authentication-architecture-decisions)
- [5. Cross-region architecture complexity](#5-cross-region-architecture-complexity)
- [6. Real-time versus batch processing trade-offs](#6-real-time-versus-batch-processing-trade-offs)
- [Summary of key learnings](#summary-of-key-learnings)

## 1. OpenSearch VPC deployment complexity

### Challenge

Running Amazon OpenSearch Service inside a VPC caused access and networking problems that threatened to block
dashboard development and testing.

### Specific issues

- An OpenSearch cluster deployed in a VPC cannot be reached directly from the internet.
- The OpenSearch Dashboards UI could not be reached for development and testing.
- API access needed a complex VPC endpoint configuration.
- The network security groups and route tables were intricate to configure.

### Impact

- Dashboard development was delayed by 2+ weeks.
- Local development and testing got more complicated.
- Troubleshooting and maintenance got more complex.

### Solution, built by the team

A teammate solved this networking problem with a PrivateLink-based solution that uses Cognito authentication. It
combines AWS PrivateLink, Network Load Balancers, and Lambda functions that track the cluster's changing IP addresses,
to give secure access to the VPC-deployed OpenSearch cluster without exposing it to the public internet.

**Key technologies:** PrivateLink, an internal access tool, Cognito federated authentication, and automated IP
tracking for the cluster's dynamic IP addresses.

### Lessons learned

- Plan a VPC deployment from the start of the project; do not retrofit it.
- Complex networking needs dedicated expertise and a lot of time.
- Team collaboration is essential for infrastructure problems that go beyond one person's expertise.

## 2. Managed Flink Studio notebook versus streaming application

### Challenge

Amazon Managed Service for Apache Flink offers two ways to deploy: **Studio notebooks** (an interactive,
Jupyter-like environment) and **streaming applications** (persistent JAR deployments). Development went through
Studio notebooks first. Each step showed more limits, and in the end the work had to move completely to a Java
streaming application.

### The two deployment options

- **Studio notebooks:** an interactive, Jupyter-like environment whose cells are run one by one. It supports PyFlink
  with SQL.
- **Streaming applications:** persistent, JAR-based deployments made for production workloads.

### Phase 1: a single sink works in a Studio notebook

**First approach.** Development began in a Studio notebook with PyFlink and SQL, writing to a single OpenSearch index
with basic username and password authentication. **This first approach worked.**

**Version compatibility problems:**

- Studio notebooks are limited to Flink 1.15, which does not support the new OpenSearch connector.
- **Workaround:** use the Elasticsearch connector in place of the OpenSearch connector.
- **Cost of the workaround:** OpenSearch had to be downgraded to version 1.3 or lower.

**Move to a streaming application.** With the single sink working, the next step was to build it as a JAR and deploy
it as a streaming application that runs all the time. **This move worked.**

### Phase 2: several sinks expose a limit

**More than one OpenSearch index.** The next requirement was several sinks, one per OpenSearch index. With several
INSERT statements, the job got stuck in an infinite loop on the first INSERT during persistent execution and never
reached the next ones.

**The documentation confirmed it.** The Studio notebook documentation says plainly that **"only one sink is allowed
for Studio Notebooks"**.

### Phase 3: a workaround

**Editing the generated source code by hand:**

1. **Build:** generate the ZIP package in the Studio notebook environment (it includes all dependencies).
2. **Get the source:** download and extract the ZIP to reach the `source/note.py` file.
3. **Understand it:** this file holds the converted Python code, where each SQL query becomes its own statement set,
   created and run independently.
4. **Edit it:** change the file so that all the sink INSERT statements sit in a single statement set.
5. **Run in parallel:** with that change, Flink runs the sinks in parallel, not one after another.

**Result:** several sinks working, through a manual change to the source code.

### Phase 4: limits with no workaround

The multi-sink workaround worked, but two basic blockers appeared that could not be solved inside the Studio notebook
environment.

**Limit 1, out-of-order event protection.** Conditional updates based on timestamps, which stop out-of-order events
from making the data go backwards, were impossible with the functions available in a Studio notebook. Data
consistency in distributed processing depends on them.

**Limit 2, authentication.** The Python Table API in Studio notebooks allowed only basic username and password
authentication. AWS IAM authentication was not possible, a serious security gap for a production deployment.

### Phase 5: a full move to a Java streaming application

**Decision.** These two limits meant dropping the Studio notebook approach entirely and building a Java solution from
scratch.

**Impact of the move:**

- **Time:** 3+ weeks of Studio notebook development and tuning had to start over.
- **Architecture:** from the Python Table API to the Java DataStream API, deployed as a streaming application.
- **Approach:** a full reimplementation, not a step-by-step upgrade.

**Technical gains:**

- full AWS SDK integration, which made IAM authentication possible;
- complete control over OpenSearch indexing patterns and conditional updates;
- advanced stateful processing for change detection;
- native support for several sinks, with no manual source changes;
- production-grade reliability and full monitoring.

### Lessons learned

- **Development strategy:** Studio notebooks are excellent for prototypes and simple cases, but they hit basic limits
  with complex enterprise requirements.
- **Deployment planning:** a multi-sink pattern needs a careful choice of Flink deployment approach during the
  architecture phase.
- **Security first:** authentication and security requirements should drive early technology decisions, not be dealt
  with afterwards.
- **Environment limits:** an interactive development environment can hide limits that matter in production and only
  show up during scaling or security hardening.

## 3. Custom OpenSearch sink development and Painless scripts

### Challenge

Building several specialized OpenSearch sinks for different data access patterns, each needing its own Painless
script logic for data consistency and life cycle management.

### Specific requirements

- **Current state index:** upserts, with protection against out-of-order events.
- **Events log index:** append only, for a complete audit trail.
- **Associates index:** complex upserts with delete support, for normalized data.
- **Conditional and fast processing modes:** different trade-offs between performance and consistency.

### Technical complexities

- **Painless scripts:** writing JavaScript-like scripts for conditional updates and timestamp-based data protection.
- **Document life cycle:** handling creation, updates and logical deletes across different data models.
- **Composite keys:** managing document IDs like `opportunity_id#associate_id` for normalized analytics.
- **Out-of-order events:** stopping the data from going backwards when events arrive out of sequence.

### Solution

**AmazonOpenSearchServiceSink**, with five sink variants:

- **buildAmazonOpenSearchSink:** a basic append-only sink, for audit logs.
- **buildAmazonOpenSearchSinkWithConditionalUpdate:** conditional updates based on timestamps, with Painless scripts.
- **buildAmazonOpenSearchSinkWithFastUpsert:** upserts at maximum performance, with no timestamp check.
- **buildAmazonOpenSearchSinkWithUpsertAndDelete:** complex life cycle management, with delete support.
- **buildAmazonOpenSearchSinkWithFastUpsertAndDelete:** the high-performance version with deletes.

### The Painless script

Conditional update logic that silently ignores older events and lets newer updates through, which prevents data
corruption in distributed processing.

### Impact

- The multi-sink pattern works, with its own behaviour per index.
- Each index can trade performance against consistency by configuration.
- Exactly-once semantics, despite the problems of distributed processing.
- Complex analytics, through correct data life cycle management.

## 4. Authentication architecture decisions

### Challenge

The standard Flink OpenSearch connectors supported only basic username and password authentication, which was not
enough for production security requirements or AWS best practice.

### Security requirements

- AWS IAM role-based authentication, with no stored credentials;
- no hard-coded credentials in application code or configuration;
- authentication between services with AWS Signature Version 4 signing.

### Implementation challenges

- The standard connectors had no AWS Signature Version 4 signing.
- IAM authentication needed a custom HTTP client.
- The AWS SDK had to be integrated while staying compatible with Flink serialization.
- Testing the authentication flows across development and production environments was complex.

### Solution: a custom sink

- **AmazonOpenSearchServiceSink:** a custom sink with full AWS integration.
- **Two authentication modes:** IAM (production) and basic auth (development).
- **Signature Version 4:** full AWS request signing for secure API access.

### Security benefits

- **No credential exposure:** no passwords or API keys in code or configuration.
- **Least-privilege access:** fine-grained IAM policies with only the permissions needed.

## 5. Cross-region architecture complexity

### Challenge

A cross-region architecture (SNS in ap-southeast-2, processing in us-east-1) added networking, latency and
operational complexity.

### Technical challenges

- cross-region SNS subscription configuration and IAM permissions;
- error handling spread across AWS regions;
- CloudWatch monitoring and alerting across regions.

### Operational challenges

- coordinating deployments and updates across regions;
- bringing monitoring and alerting together into one operational view.

### Solution

- **Dual-path architecture:** separate paths for the raw backup (Firehose) and for real-time processing.
- **Regional error handling:** a DLQ strategy for each region, with automated replay for the Lambda DLQ.
- **Unified monitoring:** a cross-region CloudWatch dashboard that brings all the metrics together.

## 6. Real-time versus batch processing trade-offs

### Challenge

Balancing the real-time requirements against cost and operational simplicity, especially when considering Kinesis
Data Firehose as an alternative.

### Firehose limits for this real-time use case

- **Built-in latency:** buffering delays of 1–15 minutes conflict with the requirement of under 2 minutes.
- **Limited transformations:** no complex business logic for computed fields.
- **No exactly-once semantics:** data can be duplicated during failures.
- **Static processing:** changing business rules need a redeployment.

### Solution: a hybrid architecture

- **Real-time path:** SNS → Lambda → Kinesis → Flink → OpenSearch, for immediate processing.
- **Backup path:** SNS → Firehose → S3, for cheap long-term storage.
- **Both together:** real-time insight, with a durable backup for compliance and recovery.

## Summary of key learnings

1. **Infrastructure planning:** plan complex networking (VPC, PrivateLink) from the start of the project.
2. **Technology selection:** let authentication and security requirements drive early technology decisions.
3. **Team collaboration:** infrastructure problems often need expertise beyond one person's knowledge.
4. **Ecosystem maturity:** connector and library support differs a lot between programming languages.
5. **Hybrid approaches:** combining approaches (real time and batch) can give the best solution.
6. **Custom development:** enterprise requirements may need a custom implementation in place of a standard connector.

These problems and their solutions show how complex it is to build a production-grade real-time streaming platform,
and how much careful technical evaluation and team collaboration matter.
