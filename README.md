# Real-Time Streaming Data Platform

**Author:** Saahil Sanganeriya  

### Description

This CR introduces a comprehensive real-time streaming data processing application using Apache Flink to handle shift opportunity events across Amazon's workforce management systems. The application processes complex nested JSON events from Amazon Kinesis Data Streams and transforms them into multiple specialized output formats for real-time dashboards, compliance auditing, and associate-level analytics.

## Overview

This platform processes shift opportunity events in real-time, transforming batch processing workflows into streaming architecture with sub-second latency.

## Architecture Components

### Core Processing
- **Flink Application**: [`shift-opportunity-consumer-amazon-managed-service-for-apache-flink/`](./shift-opportunity-consumer-amazon-managed-service-for-apache-flink/) - Java-based stream processor with multi-sink pattern
- **Unified Lambda Processor**: [`lambda-functions/unified-processor/`](./lambda-functions/unified-processor/) - SNS-to-Kinesis bridge with error handling

### Infrastructure & Configuration
- **CloudFormation Templates**: [`cloudformation-templates/`](./cloudformation-templates/) - Infrastructure as code
- **IAM Roles**: [`iam-roles/`](./iam-roles/) - Flink application permissions

### Monitoring & Testing
- **OpenSearch Dashboards**: [`opensearch dashboards/`](./opensearch%20dashboards/) - Business operations and system health dashboards
- **Data Simulator**: [`lambda-functions/ShiftOppSimulator-stack-DataSimulator.py`](./lambda-functions/ShiftOppSimulator-stack-DataSimulator.py) - Realistic test data generation

## Quick Start

1. **Deploy Infrastructure**: Use CloudFormation templates for AWS resources
2. **Configure Flink**: Deploy the Java application to Amazon Managed Service for Apache Flink
3. **Setup Lambda**: Deploy unified processor with appropriate IAM permissions
4. **Import Dashboards**: Load OpenSearch dashboard configurations
5. **Test**: Use the data simulator to generate test events

## Component Details

Each component contains its own README with specific setup and configuration instructions:

- [`shift-opportunity-consumer-amazon-managed-service-for-apache-flink/README.md`](./shift-opportunity-consumer-amazon-managed-service-for-apache-flink/README.md) - Flink application setup
- [`lambda-functions/unified-processor/README.md`](./lambda-functions/unified-processor/README.md) - Lambda processor configuration

## Data Flow

**SNS Events** → **Lambda Processor** → **Kinesis Streams** → **Flink Processing** → **OpenSearch (3 indexes)** → **Real-time Dashboards**

---
