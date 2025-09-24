# Shift Opportunity Stream Processing - Technical Architecture

## Overview

This document provides a comprehensive technical breakdown of the **Shift Opportunity Stream Processing Application** - a real-time Apache Flink streaming application that processes shift opportunity events and maintains multiple synchronized views in Amazon OpenSearch Service.

**What this application does in simple terms:**
When someone creates, updates, or deletes a shift opportunity (a work shift that needs to be filled), this application receives that event in real-time, processes it, and stores it in multiple specialized formats to support different business needs like dashboards, compliance auditing, and individual worker analytics.

---

## Table of Contents

1. [System Architecture & Data Flow](#system-architecture--data-flow)
2. [Event Journey: From Creation to Storage](#event-journey-from-creation-to-storage)
3. [Input Data Structure](#input-data-structure)
4. [Data Transformations](#data-transformations)
5. [Output Destinations](#output-destinations)
6. [Key Technical Decisions](#key-technical-decisions)
7. [File Structure & Responsibilities](#file-structure--responsibilities)
8. [Configuration & Deployment](#configuration--deployment)
9. [Error Handling & Reliability](#error-handling--reliability)
10. [Performance & Monitoring](#performance--monitoring)

---

## System Architecture & Data Flow

### High-Level Architecture

```
┌─────────────┐    ┌─────────┐    ┌─────────────────┐    ┌────────────────────┐
│   Source    │    │  SNS    │    │     Lambda      │    │    Kinesis Data    │
│   System    │───▶│ Topic   │───▶│  Transformer    │───▶│     Streams        │
│ (Business)  │    │         │    │                 │    │                    │
└─────────────┘    └─────────┘    └─────────────────┘    └────────────────────┘
                                                                      │
                                                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        FLINK PROCESSING APPLICATION                          │
│  ┌─────────────┐   ┌──────────────────┐   ┌─────────────────────────────┐    │
│  │   Kinesis   │──▶│   Deserialization │──▶│       Transformations       │    │
│  │   Source    │   │     & Parsing    │   │    ┌─────────────────────┐   │    │
│  └─────────────┘   └──────────────────┘   │    │  1. Current State   │   │    │
│                                           │    │  2. Audit Log       │   │    │
│                                           │    │  3. Associate View  │   │    │
│                                           │    └─────────────────────┘   │    │
│                                           └─────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    ▼                      ▼                      ▼
          ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
          │   OpenSearch    │    │   OpenSearch    │    │   OpenSearch    │
          │ Current State   │    │  Events Log     │    │  Associates     │
          │     Index       │    │     Index       │    │     Index       │
          │ (Dashboard)     │    │  (Compliance)   │    │  (Analytics)    │
          └─────────────────┘    └─────────────────┘    └─────────────────┘
                                           │
                                           ▼
                                 ┌─────────────────┐
                                 │       S3        │
                                 │ Backup Storage  │
                                 │ (Long-term)     │
                                 └─────────────────┘
```

### Core Concept: Multi-Sink Pattern

This application implements a **"multi-sink pattern"** where each incoming event gets transformed into multiple output formats:

1. **Current State**: Latest information for real-time dashboards
2. **Audit Log**: Complete history for compliance and debugging  
3. **Associate Records**: Individual worker assignments for analytics
4. **Backup Storage**: Long-term retention in S3

---

## Event Journey: From Creation to Storage

### Step 1: Event Generation
When a business user creates, updates, or deletes a shift opportunity in the source system, an event is published to an Amazon SNS topic.

### Step 2: Event Enrichment  
An AWS Lambda function receives the SNS notification, enriches it with metadata (timestamps, message IDs), and forwards it to Kinesis Data Streams.

### Step 3: Stream Ingestion
The Flink application continuously reads events from Kinesis Data Streams in real-time.

### Step 4: Deserialization
Raw JSON bytes are parsed into Java objects using custom logic that handles complex nested structures and edge cases.

### Step 5: Data Quality Filtering
Events are validated to ensure they have required fields (like opportunity ID) before processing.

### Step 6: Parallel Transformations
Each valid event is simultaneously transformed into three different formats using parallel processing.

### Step 7: Multi-Destination Output
The transformed events are written to multiple destinations with different patterns:
- **Upsert** to current state index (latest data only)
- **Append** to audit log index (complete history)
- **Upsert with deletion** to associates index (normalized view)
- **Backup** to S3 files (disaster recovery)

---

## Input Data Structure

### Raw Event Format
Events arrive as complex JSON structures wrapped in SNS metadata:

```json
{
  "metadata": {
    "messageId": "12345-abcde-67890",
    "sns_timestamp": "2024-01-01T12:00:00Z",
    "lambda_timestamp": "2024-01-01T12:00:01Z",
    "topicArn": "arn:<partition>:sns:us-east-1:123456789012:ShiftOpportunityServiceEvents",
    "attributes": {
      "EventType": {"Value": "OpportunityUpdated"},
      "Country": {"Value": "US"},
      "OpportunityType": {"Value": "REGULAR"},
      "FieldsChanged": {"Value": "totalHeadCount,acceptedAssociateIds"}
    }
  },
  "message": {
    "entityId": "opportunity-123",
    "eventTime": "2024-01-01T11:59:59Z",
    "sequenceNumber": 42,
    "event": {
      "serializationKey": "opportunity-123",
      "serializedData": {
        "newOpp": { /* Current opportunity state */ },
        "oldOpp": { /* Previous opportunity state (for updates) */ }
      }
    }
  }
}
```

### Key Data Processing Logic

**COALESCE Strategy**: The application uses smart logic to extract the current state:
- **Created Events**: Data comes from `newOpp`, `oldOpp` is null
- **Updated Events**: Current state from `newOpp`, previous state from `oldOpp`  
- **Deleted Events**: Final state comes from `oldOpp`, `newOpp` is null

**Business Field Computation**: The application calculates derived fields:
- `fillStatus`: OPEN, PARTIALLY_FILLED, or FILLED (based on headcount vs accepted associates)
- `claimStatus`: NOT_STARTED, ACTIVE, or EXPIRED (based on claim time windows)
- `headCountChanged`: Boolean flag for capacity changes
- `acceptedAssociatesChanged`: Boolean flag for assignment changes

---

## Data Transformations

### 1. Current State Transformation (`ShiftOpportunityToCurrentEvent`)

**Purpose**: Creates dashboard-optimized records for real-time monitoring

**Input**: 1 opportunity event  
**Output**: 1 current state document  
**Operation**: `map()` (one-to-one transformation)

**What it does**:
- Extracts all business fields from the input event
- Calculates processing latency metrics (end-to-end and SNS-to-Flink)
- Organizes fields in logical groups for efficient querying
- Preserves complete timestamp chain for audit purposes

**Key Fields Added**:
- `flinkTimestamp`: When processing completed
- `endToEndLatencyMs`: Total system latency  
- `snsToFlinkLatencyMs`: Real-time processing latency

### 2. Audit Log Transformation (`ShiftOpportunityToLogEvent`)

**Purpose**: Creates comprehensive audit records for compliance and debugging

**Input**: 1 opportunity event  
**Output**: 1 audit log document  
**Operation**: `map()` (one-to-one transformation)

**What it does**:
- Preserves ALL event data including SNS metadata
- Maintains complete change tracking information
- Includes previous state data for comparison
- Adds performance metrics for monitoring

**Additional Data Preserved**:
- SNS message ID for correlation
- Topic ARN for source identification  
- Sequence number for event ordering
- Complete field change tracking
- Previous state information

### 3. Associate Expansion Transformation (`ShiftOpportunityToAssociateEvents`)

**Purpose**: Creates individual records for each worker assignment

**Input**: 1 opportunity event  
**Output**: N associate documents (where N = number of associates)  
**Operation**: `flatMap()` (one-to-many transformation)

**What it does**:
- **Normalization**: Converts one opportunity with multiple associates into separate records
- **Change Detection**: Compares current vs previous associate lists
- **Deletion Handling**: Creates deletion records for removed associates
- **Composite IDs**: Generates unique document IDs (opportunity_id + "#" + associate_id)

**Complex Logic Example**:
```
Input: Opportunity with associates ["worker1", "worker2", "worker3"]
Previous: ["worker1", "worker2"]

Output:
- UPDATE record for worker1 (still assigned)
- UPDATE record for worker2 (still assigned)  
- CREATE record for worker3 (newly assigned)
```

---

## Output Destinations

### 1. Current State Index (`shift-opportunity-current`)

**Access Pattern**: Upsert (Update or Insert)  
**Primary Key**: `opportunity_id`  
**Update Strategy**: Conditional updates with timestamp protection  
**Use Cases**: Real-time dashboards, alerting, current status queries

**Data Characteristics**:
- Latest state only (previous versions overwritten)
- Optimized field ordering for user interfaces
- Business status fields (claim_status, fill_status)
- Performance metrics for monitoring

**Why Conditional Updates**: Prevents data corruption from out-of-order events using Painless scripts that only update if the new timestamp is newer than existing.

### 2. Events Log Index (`shift-opportunity-events-log`)

**Access Pattern**: Append-only  
**Primary Key**: None (auto-generated)  
**Update Strategy**: Insert only  
**Use Cases**: Compliance auditing, debugging, historical analysis

**Data Characteristics**:
- Complete event history preservation
- SNS metadata for debugging
- Change tracking flags
- Previous state information
- Immutable audit trail

### 3. Associates Index (`shift-opportunity-associates`)

**Access Pattern**: Upsert with deletion support  
**Primary Key**: `document_id` (opportunity_id + "#" + associate_id)  
**Update Strategy**: Conditional updates with deletion handling  
**Use Cases**: Associate analytics, workload distribution, capacity planning

**Data Characteristics**:
- One record per worker assignment
- Normalized data model
- Deletion flag support (`isDeleted`)
- Worker-specific analytics fields

**Complex Deletion Logic**: When associates are removed from opportunities, deletion records (`isDeleted: true`) are sent to properly clean up the index.

### 4. S3 Backup Storage (Optional)

**Access Pattern**: Time-partitioned files  
**File Format**: JSON Lines  
**Rolling Policy**: 15-minute intervals or 128MB file size  
**Use Cases**: Long-term retention, disaster recovery, batch analytics

---

## Key Technical Decisions

### 1. Why Custom OpenSearch Sink?

**Problem**: The standard Flink OpenSearch connector doesn't support:
- AWS IAM authentication (only basic auth)
- Conditional updates to prevent data corruption
- Deletion operations for complex lifecycles

**Solution**: Custom sink implementation (`AmazonOpenSearchServiceSink`) that provides:
- **AWS IAM Integration**: Uses AWS Signature v4 signing for secure access
- **Painless Scripts**: JavaScript-like scripts for conditional updates
- **Deletion Support**: Proper handling of record removal

### 2. Why Painless Scripts for Updates?

**Problem**: Distributed streaming can cause events to arrive out of order, potentially overwriting newer data with older events.

**Our Approach**: Painless scripts that silently ignore older events

```javascript
// Painless script logic
if (ctx._source == null || params.new_timestamp.compareTo(ctx._source.src_timestamp) >= 0) {
  ctx._source = params.doc;  // Update with new data
} else {
  ctx.op = 'noop';          // Silently ignore older event
}
```

**Benefits**:
- No pipeline failures from out-of-order events
- Strict data consistency protection
- Silent handling of conflicts

### 3. Why Multi-Sink Pattern?

**Business Need**: Different teams need different views of the same data:
- **Operations**: Current state for monitoring
- **Compliance**: Complete audit trail  
- **Analytics**: Individual worker metrics

**Technical Benefits**:
- Specialized indexes for different query patterns
- Independent scaling and optimization
- Data redundancy for resilience

### 4. Why FlatMap for Associates?

**Business Reality**: One opportunity can have multiple workers assigned, and we need to track individual worker performance.

**Technical Challenge**: Relational data (one-to-many) in a document store

**Solution**: Normalize by creating one document per worker assignment, enabling efficient analytics queries across all worker assignments.

---

## File Structure & Responsibilities

### Core Application Files

#### `ProcessShiftOpportunityStream.java` - **Main Orchestrator**
- **What it does**: The "conductor" that sets up and runs the entire processing pipeline
- **Key responsibilities**:
  - Configures Kinesis data source
  - Sets up all transformations  
  - Creates OpenSearch sinks
  - Handles configuration and deployment differences
  - Manages the complete data flow

#### Input Data Model

##### `ShiftOpportunityEvent.java` - **Input Event Parser**
- **What it does**: Converts raw JSON from Kinesis into a structured Java object
- **Key features**:
  - Handles complex nested JSON structures
  - Implements COALESCE logic for different event types
  - Computes business status fields
  - Manages timestamp extraction and validation

##### `EventDeserializationSchema.java` - **Flink Integration Bridge**
- **What it does**: Connects the custom JSON parser to Flink's streaming system
- **Key features**:
  - Error-resilient parsing (bad events don't crash the pipeline)
  - Debug logging for troubleshooting
  - Type safety for Flink's serialization system

#### Output Data Models

##### `Document.java` - **Base JSON Serializer**
- **What it does**: Provides consistent JSON formatting for all output formats
- **Key features**:
  - Snake-case field naming (opportunity_id vs opportunityId)
  - Proper timestamp formatting
  - Thread-safe JSON serialization

##### `ShiftOpportunityCurrentEvent.java` - **Dashboard Format**
- **What it does**: Optimized data structure for real-time dashboards
- **Key features**:
  - User-friendly field ordering
  - Performance metrics included
  - Optimized for frequent updates

##### `ShiftOpportunityLogEvent.java` - **Audit Format**  
- **What it does**: Comprehensive audit record with complete event history
- **Key features**:
  - Preserves all metadata
  - Change tracking flags
  - Previous state information

##### `ShiftOpportunityAssociateEvent.java` - **Analytics Format**
- **What it does**: Individual worker assignment records for detailed analytics
- **Key features**:
  - Composite document IDs
  - Deletion flag support
  - Worker-specific fields

#### Transformation Logic

##### `ShiftOpportunityToCurrentEvent.java` - **Dashboard Transformer**
- **What it does**: Converts input events to dashboard-optimized format
- **Operation**: One-to-one mapping with latency calculations

##### `ShiftOpportunityToLogEvent.java` - **Audit Transformer**
- **What it does**: Converts input events to comprehensive audit format  
- **Operation**: One-to-one mapping with complete data preservation

##### `ShiftOpportunityToAssociateEvents.java` - **Associate Expander**
- **What it does**: Expands one opportunity into multiple worker records
- **Operation**: One-to-many transformation with change detection
- **Complex logic**: **354 lines** handling additions, removals, and deletions

#### Infrastructure Components

##### `AmazonOpenSearchServiceSink.java` - **Custom Sink Implementation**
- **What it does**: Handles all OpenSearch output operations
- **Key features**:
  - AWS IAM authentication
  - Conditional update scripts
  - Deletion support
  - Multiple sink variants (fast vs safe)

##### `ParameterToolUtils.java` - **Configuration Bridge**
- **What it does**: Converts Amazon MSF application properties to Flink parameters
- **Why needed**: Bridge between AWS deployment format and Flink's configuration system

### Configuration Files

#### `log4j2.properties` - **Logging Configuration**
- **What it does**: Defines logging levels and output formats
- **Key features**:
  - Application-specific logging for debugging
  - CloudWatch integration for production monitoring
  - Performance-optimized patterns

#### `testProperties.json` - **Development Configuration**
- **What it does**: Example configuration for local development and testing
- **Contains**: Stream ARNs, OpenSearch endpoints, authentication settings

#### `pom.xml` - **Build Configuration**
- **What it does**: Defines all dependencies and build settings
- **Key dependencies**:
  - Flink 1.20.0 for stream processing
  - AWS Kinesis connector for data ingestion
  - Custom OpenSearch libraries for output
  - Jackson/Gson for JSON processing
  - AWS SDK for IAM authentication

---

## Configuration & Deployment

### Deployment Modes

#### Local Development
```bash
# Parameters passed via command line
--InputStreamArn arn:<partition>:kinesis:us-east-1:123:stream/test
--OpenSearchEndpoint https://search-dev.us-east-1.es.amazonaws.com
```

#### Production (Amazon MSF)
```json
{
  "PropertyGroupId": "FlinkApplicationProperties",
  "PropertyMap": {
    "InputStreamArn": "arn:<partition>:kinesis:us-east-1:123456789012:stream/ShiftOpportunityServiceEvents",
    "OpenSearchEndpoint": "https://search-prod.us-east-1.es.amazonaws.com:443",
    "ProcessingMode": "conditional",
    "Region": "us-east-1"
  }
}
```

### Key Configuration Parameters

#### Required Parameters
- **`InputStreamArn`**: Source Kinesis stream for event ingestion
- **`OpenSearchEndpoint`**: Target OpenSearch cluster URL

#### Authentication Options
- **IAM (Recommended)**: Automatic credential resolution using IAM roles
- **Basic Auth**: Username/password (development only)

#### Processing Modes
- **`conditional`** (Default): Safe mode with out-of-order protection
- **`fast`**: Maximum speed mode without timestamp validation

#### Optional Features
- **S3 Backup**: Long-term retention with configurable bucket and prefix
- **Custom Index Names**: Override default OpenSearch index names

---

## Error Handling & Reliability

### Data Quality Protection

#### Input Validation
- **Required Field Checking**: Events missing opportunity IDs are filtered out
- **JSON Parsing Errors**: Malformed events are logged and skipped
- **Graceful Degradation**: Bad events don't stop processing of good events

#### Out-of-Order Event Protection
- **Conditional Updates**: Only newer timestamps can update existing documents
- **Silent Handling**: Older events are ignored without errors
- **Data Consistency**: Prevents data regression in distributed processing

### Fault Tolerance

#### Flink Checkpointing
- **Automatic State Backup**: Every 10 seconds
- **Failure Recovery**: Automatic restart from last checkpoint
- **Exactly-Once Processing**: No data loss or duplication

#### Error Isolation
- **Per-Event Error Handling**: One bad event doesn't affect others
- **Sink Independence**: Failure in one output doesn't affect others
- **Retry Logic**: Automatic retries with exponential backoff

---

## Performance & Monitoring

### Latency Metrics

The application tracks comprehensive timing information:

#### End-to-End Latency
- **Measurement**: Source system timestamp to Flink processing completion
- **Typical Range**: 1-7 seconds for normal operations
- **Alert Threshold**: >30 seconds indicates system issues

#### Processing Pipeline Latency  
- **Measurement**: SNS publication to Flink processing completion
- **Typical Range**: 500ms-2 seconds
- **Components**: SNS → Lambda → Kinesis → Flink processing

### Throughput Characteristics

#### Design Targets
- **Event Rate**: Thousands of events per hour with burst capability
- **Processing**: Sub-second individual event processing
- **Throughput**: Scales horizontally with Flink parallelism

#### Optimization Features
- **Real-time Processing**: Immediate flush after each record
- **Bulk Operations**: Configurable batching for high throughput scenarios
- **Parallel Processing**: Independent transformation pipelines

### Monitoring Integration

#### CloudWatch Logs
- **Application Logs**: DEBUG level for detailed troubleshooting
- **Infrastructure Logs**: INFO level for operational monitoring
- **Structured Format**: Easy parsing and alerting

#### Performance Dashboards
- **Current State Index**: Real-time business dashboards
- **Latency Metrics**: Built into every processed event
- **Error Rates**: Tracking and alerting on processing failures

---

## Summary

This Flink application solves a complex real-time data processing challenge by:

1. **Ingesting** complex JSON events from Kinesis streams
2. **Parsing** nested data structures with robust error handling
3. **Transforming** events into multiple specialized formats
4. **Outputting** to multiple destinations with different access patterns
5. **Ensuring** data consistency despite distributed processing challenges
6. **Providing** comprehensive monitoring and debugging capabilities

The application demonstrates advanced stream processing patterns including multi-sink architectures, conditional updates, data normalization, and fault-tolerant processing - all while maintaining sub-second latency and exactly-once processing guarantees.