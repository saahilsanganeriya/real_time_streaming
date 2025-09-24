# Real-Time Shift Opportunity Stream Processing Application

**Author:** Saahil Sanganeriya  
**Project Timeline:** May 27, 2025 - August 15, 2025  

## Overview

This Apache Flink application processes real-time shift opportunity events from Amazon Kinesis Data Streams and transforms them into structured data for storage in Amazon OpenSearch Service. The application implements a production-ready streaming architecture with fault tolerance, scalability, and comprehensive data processing capabilities.

### Key Features

- **Real-time Processing**: Sub-second latency from Kinesis ingestion to OpenSearch indexing
- **Dual-Sink Pattern**: Current state management (upsert) + complete audit trail (append-only)
- **Associate Expansion**: Detailed analytics with one record per associate assignment
- **Out-of-Order Protection**: Configurable timestamp-based conditional updates
- **AWS IAM Integration**: Secure authentication with AWS OpenSearch Service
- **S3 Backup**: Long-term data retention and disaster recovery
- **Production-Ready**: Comprehensive monitoring, logging, and error handling

## Architecture Overview

### Data Flow

```
SNS Topic → Lambda → Kinesis Data Streams → Flink Application → OpenSearch Service
                                         ↓
                                        S3 (Backup)
```

1. **Event Ingestion**: Cross-region SNS topic publishes shift opportunity events
2. **Lambda Processing**: Reformats SNS messages, adds metadata, generates partition keys, puts data on kinesis stream
3. **Stream Processing**: Flink application fetches data from stream, transforms nested JSON into multiple output formats
4. **Real-time Storage**: Dual-sink to OpenSearch for current state and audit logging
5. **Backup Storage**: S3 for long-term retention and batch analytics

### OpenSearch Indexes

| Index | Pattern | Purpose | Key Features |
|-------|---------|---------|--------------|
| `shift-opportunity-current` | Upsert | Real-time dashboards | Latest state per opportunity, timestamp-based conditional updates |
| `shift-opportunity-events-log` | Append | Audit trail | Complete event history, compliance, debugging |
| `shift-opportunity-associates` | Upsert + Delete | Associate analytics | One record per (opportunity, associate) with deletion support |

## Technology Choices & Architecture Decisions

### Why Java + DataStream API?

**Chosen:** Java with Flink DataStream API  
**Alternative:** Python with Table API

**Reasons:**
1. **Feature Completeness**: DataStream API provides full access to Flink's capabilities
2. **Documentation & Community**: Extensive documentation and examples in Java
3. **Performance**: Better performance than Python for stream processing workloads
4. **Control**: Fine-grained control over transformations and state management
5. **Production Readiness**: More mature ecosystem for production deployments

### Why Custom OpenSearch Sink?

**Problem:** Standard Flink OpenSearch connector doesn't support AWS IAM authentication

**Solution:** Custom sink with AWS Signature v4 signing

**Key Features:**
- AWS IAM authentication for secure, credential-free access
- Timestamp-based conditional updates using Painless scripts
- Out-of-order event protection
- Bulk operations for performance
- Comprehensive error handling and retries

### Why Not OpenSearch Ingestion/Transforms?

**OpenSearch Ingestion Limitations:**
- Limited transformation capabilities for complex nested JSON
- No support for dual-sink patterns with different indexing strategies
- Reduced control over error handling and monitoring
- Less flexibility for business logic implementation

**OpenSearch Transform Limitations:**
- Batch-oriented processing (not real-time)
- Limited integration with upstream Kinesis streams
- No support for complex event-time processing and watermarking

### Processing Modes

#### Conditional Mode (Recommended for Production)
- **Timestamp-based conditional updates** using Painless scripts
- **Prevents data regression** from out-of-order events
- **Ensures data consistency** in distributed processing
- **Slightly higher latency** due to timestamp validation

#### Fast Mode (Maximum Throughput)
- **Direct upserts** without timestamp validation
- **Maximum processing speed** and throughput
- **Risk of data regression** if events arrive out-of-order
- **Suitable for scenarios** where event ordering is guaranteed

## Project Structure

```
src/main/java/com/amazon/shift/opportunity/
├── ProcessShiftOpportunityStream.java     # Main application entry point
├── model/
│   ├── Document.java                      # Base class for OpenSearch documents
│   ├── ShiftOpportunityEvent.java         # Input event model (from Kinesis)
│   ├── ShiftOpportunityCurrentEvent.java  # Current state model
│   ├── ShiftOpportunityLogEvent.java      # Audit log model
│   └── ShiftOpportunityAssociateEvent.java # Associate expansion model
├── transform/
│   ├── ShiftOpportunityToCurrentEvent.java      # Current state transformation
│   ├── ShiftOpportunityToLogEvent.java          # Log event transformation
│   └── ShiftOpportunityToAssociateEvents.java   # Associate expansion transformation
├── sink/
│   └── AmazonOpenSearchServiceSink.java         # Custom OpenSearch sink
├── serialization/
│   └── EventDeserializationSchema.java   # Kinesis deserialization
└── util/
    └── ParameterToolUtils.java           # Configuration utilities
```

## Event Processing Logic

### Event Types

1. **OpportunityCreated**: New opportunities published to the system
   - `newOpp` contains opportunity data, `oldOpp` is null
   - No previous state tracking

2. **OpportunityUpdated**: Changes to existing opportunities
   - `newOpp` contains current state, `oldOpp` contains previous state
   - Change detection and field-level tracking

3. **OpportunityDeleted**: Opportunity removal
   - `oldOpp` contains final state, `newOpp` is null
   - Triggers associate record cleanup

### Data Transformations

#### 1. Current State Events
- **Purpose**: Real-time dashboard queries
- **Pattern**: Upsert by `opportunityId`
- **Features**: Derived fields (claim_status, fill_status), latency metrics

#### 2. Audit Log Events
- **Purpose**: Compliance and debugging
- **Pattern**: Append-only (no primary key)
- **Features**: Complete event history, change tracking, metadata preservation

#### 3. Associate Events
- **Purpose**: Associate-level analytics
- **Pattern**: Upsert by composite key (opportunity_id + associate_id)
- **Features**: One record per assignment, supports deletions, normalized data model

## Configuration

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `InputStreamArn` | Kinesis Data Stream ARN | `arn:<partition>:kinesis:us-east-1:123:stream/ShiftOpportunityServiceEvents` |
| `OpenSearchEndpoint` | OpenSearch cluster endpoint | `https://search-domain.us-east-1.es.amazonaws.com:443` |

### Optional Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ProcessingMode` | `conditional` | Processing mode: `conditional` or `fast` |
| `Region` | `us-east-1` | AWS region for IAM authentication |
| `OpenSearchUsername` | - | Basic auth username (not recommended for production) |
| `OpenSearchPassword` | - | Basic auth password (not recommended for production) |
| `S3BucketName` | - | S3 bucket for backup storage |
| `S3Prefix` | `shift-opportunity-logs` | S3 key prefix |
| `OpenSearchCurrentStateIndex` | `shift-opportunity-current` | Current state index name |
| `OpenSearchEventsLogIndex` | `shift-opportunity-events-log` | Events log index name |
| `OpenSearchAssociatesIndex` | `shift-opportunity-associates` | Associates index name |

### Configuration Examples

#### Local Development (testProperties.json)
```json
[
  {
    "PropertyGroupId": "FlinkApplicationProperties",
    "PropertyMap": {
      "InputStreamArn": "arn:<partition>:kinesis:us-east-1:123456789012:stream/ShiftOpportunityServiceEvents",
      "OpenSearchEndpoint": "https://search-your-domain.us-east-1.es.amazonaws.com:443",
      "ProcessingMode": "conditional",
      "Region": "us-east-1"
    }
  }
]
```

#### Production (MSF Application Properties)
```json
{
  "InputStreamArn": "arn:<partition>:kinesis:us-east-1:prod-account:stream/ShiftOpportunityServiceEvents",
  "OpenSearchEndpoint": "https://vpc-prod-opensearch.us-east-1.es.amazonaws.com:443",
  "ProcessingMode": "conditional",
  "Region": "us-east-1",
  "S3BucketName": "prod-shift-opportunity-backup",
  "S3Prefix": "shift-opportunity-logs"
}
```

## Building and Deployment

### Prerequisites

- Java 11 or higher
- Apache Maven 3.6+
- AWS CLI configured with appropriate permissions

### Build Application

```bash
# Clean and compile
mvn clean compile

# Create deployment JAR
mvn package
```

This creates `target/amazon-managed-service-for-apache-flink-shift-opportunity-consumer-1.0-SNAPSHOT.jar`

### Local Development

```bash
# Run locally with test configuration
java -cp target/amazon-managed-service-for-apache-flink-shift-opportunity-consumer-1.0-SNAPSHOT.jar \
  com.amazon.shift.opportunity.ProcessShiftOpportunityStream \
  --InputStreamArn arn:<partition>:kinesis:us-east-1:123:stream/test-stream \
  --OpenSearchEndpoint https://search-test.us-east-1.es.amazonaws.com:443
```

### Production Deployment

1. **Upload JAR** to S3 bucket accessible by Kinesis Analytics
2. **Create MSF Application** via AWS Console or CloudFormation
3. **Configure Application Properties** with production parameters
4. **Set up IAM Roles** with required permissions
5. **Start Application** and monitor via CloudWatch
