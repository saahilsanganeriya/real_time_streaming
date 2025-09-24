# Configuration Documentation

**Real-Time Shift Opportunity Stream Processing Application**  
**Author:** Saahil Sanganeriya

## Overview

This document provides comprehensive configuration guidance for the Flink streaming application in both local development and production Amazon Managed Service for Apache Flink environments.

## Configuration Structure

The application uses Amazon MSF's standard configuration format with property groups. All configuration parameters are defined under the `FlinkApplicationProperties` group.

### Local Development Configuration

For local development, parameters are provided via command line arguments:

```bash
java -cp target/amazon-managed-service-for-apache-flink-shift-opportunity-consumer-1.0-SNAPSHOT.jar \
  com.amazon.shift.opportunity.ProcessShiftOpportunityStream \
  --InputStreamArn arn:<partition>:kinesis:us-east-1:123:stream/test-stream \
  --OpenSearchEndpoint https://search-test.us-east-1.es.amazonaws.com:443 \
  --ProcessingMode conditional
```

### Production Configuration

For production deployment, parameters are configured through the MSF console or CloudFormation. The application automatically loads these from the runtime environment.

## Configuration Parameters

### Required Parameters

#### InputStreamArn
- **Type:** String  
- **Required:** Yes  
- **Description:** ARN of the Amazon Kinesis Data Stream containing shift opportunity events
- **Example:** `arn:<partition>:kinesis:us-east-1:123456789012:stream/ShiftOpportunityServiceEvents`
- **Validation:** Must be a valid Kinesis stream ARN accessible by the application's IAM role

#### OpenSearchEndpoint  
- **Type:** String  
- **Required:** Yes  
- **Description:** Amazon OpenSearch Service cluster endpoint URL
- **Example:** `https://search-your-domain.us-east-1.es.amazonaws.com:443`
- **Format:** Must include protocol (https://) and port (443 for standard HTTPS)
- **Validation:** Must be accessible from the MSF application's VPC/subnet configuration

### Authentication Parameters

#### OpenSearchUsername
- **Type:** String  
- **Required:** No  
- **Description:** Username for basic authentication to OpenSearch
- **Usage:** Development/testing environments only
- **Security:** Not recommended for production use
- **Alternative:** Use IAM authentication by omitting this parameter

#### OpenSearchPassword
- **Type:** String  
- **Required:** No  
- **Description:** Password for basic authentication to OpenSearch
- **Usage:** Development/testing environments only  
- **Security:** Store securely, not recommended for production
- **Alternative:** Use IAM authentication by omitting this parameter

#### Region
- **Type:** String  
- **Required:** No  
- **Default:** `us-east-1`
- **Description:** AWS region for IAM authentication with OpenSearch
- **Usage:** Required when using IAM authentication (recommended)
- **Example:** `us-east-1`, `us-west-2`, `eu-west-1`

### Processing Configuration

#### ProcessingMode
- **Type:** String  
- **Required:** No  
- **Default:** `conditional`  
- **Description:** Controls event processing strategy for data consistency
- **Options:**
  - `conditional`: Timestamp-based conditional updates (recommended for production)
  - `fast`: Direct upserts without timestamp validation (maximum throughput)

**Conditional Mode (Recommended):**
- Provides out-of-order event protection
- Uses Painless scripts for timestamp comparison
- Slightly higher latency due to conditional logic
- Prevents data regression from late-arriving events
- Best for production environments

**Fast Mode:**
- Maximum processing speed and throughput
- No protection against out-of-order events
- Risk of data regression if events arrive out of sequence
- Best when event ordering is guaranteed

### S3 Backup Configuration

#### S3BucketName
- **Type:** String  
- **Required:** No  
- **Description:** S3 bucket name for backup storage and disaster recovery
- **Example:** `your-shift-opportunity-logs-bucket`
- **Usage:** If provided, enables S3 backup of all processed events
- **IAM Requirements:** Application role must have write permissions to the bucket

#### S3Prefix
- **Type:** String  
- **Required:** No  
- **Default:** `shift-opportunity-logs`
- **Description:** S3 key prefix for organizing backup files
- **Format:** Backup files are stored as `s3://bucket/prefix/yyyy/MM/dd/HH/part-files`
- **Usage:** Only relevant if S3BucketName is configured

### OpenSearch Index Configuration

#### OpenSearchCurrentStateIndex
- **Type:** String  
- **Required:** No  
- **Default:** `shift-opportunity-current`
- **Description:** Index name for current state documents (upsert pattern)
- **Usage:** Real-time dashboard queries, latest opportunity state
- **Pattern:** Upserts by opportunity_id with conditional updates

#### OpenSearchEventsLogIndex
- **Type:** String  
- **Required:** No  
- **Default:** `shift-opportunity-events-log`
- **Description:** Index name for audit trail documents (append-only pattern)
- **Usage:** Compliance, debugging, historical analysis
- **Pattern:** Append-only, no primary key, complete event history

#### OpenSearchAssociatesIndex
- **Type:** String  
- **Required:** No  
- **Default:** `shift-opportunity-associates`
- **Description:** Index name for associate-level documents (normalized pattern)
- **Usage:** Associate analytics, workload distribution analysis
- **Pattern:** Upserts by composite key (opportunity_id + associate_id) with deletion support

## Environment-Specific Configuration Examples

### Development Environment

```json
{
  "PropertyGroupId": "FlinkApplicationProperties",
  "PropertyMap": {
    "InputStreamArn": "arn:<partition>:kinesis:us-east-1:dev-account:stream/ShiftOpportunityServiceEvents-Dev",
    "OpenSearchEndpoint": "https://search-dev-cluster.us-east-1.es.amazonaws.com:443",
    "OpenSearchUsername": "dev-user",
    "OpenSearchPassword": "dev-password",
    "ProcessingMode": "fast",
    "Region": "us-east-1"
  }
}
```

### Production Environment

```json
{
  "PropertyGroupId": "FlinkApplicationProperties", 
  "PropertyMap": {
    "InputStreamArn": "arn:<partition>:kinesis:us-east-1:prod-account:stream/ShiftOpportunityServiceEvents",
    "OpenSearchEndpoint": "https://vpc-prod-cluster.us-east-1.es.amazonaws.com:443",
    "ProcessingMode": "conditional",
    "Region": "us-east-1",
    "S3BucketName": "prod-shift-opportunity-backup",
    "S3Prefix": "shift-opportunity-logs"
  }
}
```

### High-Throughput Environment

```json
{
  "PropertyGroupId": "FlinkApplicationProperties",
  "PropertyMap": {
    "InputStreamArn": "arn:<partition>:kinesis:us-east-1:account:stream/ShiftOpportunityServiceEvents",
    "OpenSearchEndpoint": "https://vpc-cluster.us-east-1.es.amazonaws.com:443", 
    "ProcessingMode": "fast",
    "Region": "us-east-1",
    "OpenSearchCurrentStateIndex": "shift-opportunity-current-v2",
    "OpenSearchEventsLogIndex": "shift-opportunity-events-log-v2",
    "OpenSearchAssociatesIndex": "shift-opportunity-associates-v2"
  }
}
```

## Security Considerations

### IAM Authentication (Recommended)

For production environments, use IAM authentication by:
1. Omitting `OpenSearchUsername` and `OpenSearchPassword` parameters
2. Configuring appropriate IAM roles and policies

**Required IAM Permissions:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator", 
        "kinesis:GetRecords",
        "kinesis:ListShards"
      ],
      "Resource": "arn:<partition>:kinesis:*:*:stream/ShiftOpportunityServiceEvents"
    },
    {
      "Effect": "Allow",
      "Action": [
        "es:ESHttpPost",
        "es:ESHttpPut", 
        "es:ESHttpDelete"
      ],
      "Resource": "arn:<partition>:es:*:*:domain/your-opensearch-domain/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl"
      ],
      "Resource": "arn:<partition>:s3:::your-backup-bucket/*"
    }
  ]
}
```

### Basic Authentication

Only use basic authentication for development/testing:
- Store credentials securely (AWS Secrets Manager, Parameter Store)
- Rotate credentials regularly
- Restrict network access to OpenSearch cluster
- Monitor access logs for unauthorized usage

### Network Security

- Deploy OpenSearch in VPC with appropriate security groups
- Use VPC endpoints for Kinesis access where possible
- Configure MSF application in private subnets

## Monitoring and Troubleshooting

### Configuration Validation

The application validates configuration parameters at startup:
- Checks for required parameters (InputStreamArn, OpenSearchEndpoint)
- Validates ARN format and accessibility
- Tests OpenSearch connectivity and authentication
- Logs configuration summary for operational visibility

### Common Configuration Issues

1. **Invalid Stream ARN**
   - Symptom: Application fails to start with authentication errors
   - Solution: Verify ARN format and IAM permissions

2. **OpenSearch Connectivity**
   - Symptom: Connection timeouts or authentication failures
   - Solution: Check VPC configuration, security groups, and IAM policies

3. **Processing Mode Mismatch**
   - Symptom: Data inconsistencies or performance issues
   - Solution: Choose appropriate mode based on event ordering guarantees

4. **S3 Backup Failures**
   - Symptom: Application runs but backup files not created
   - Solution: Verify S3 bucket permissions and accessibility

### CloudWatch Integration

Monitor application configuration and health through CloudWatch:
- Application logs: Configuration validation and startup issues
- Custom metrics: Processing rates, error counts, latency measurements
- Alarms: Configuration-related failures and performance degradation

## Configuration Management Best Practices

1. **Environment Separation**: Use separate configurations for dev/staging/prod
2. **Parameter Validation**: Implement validation in deployment pipelines
3. **Secret Management**: Use AWS Secrets Manager for sensitive parameters
4. **Documentation**: Maintain up-to-date configuration documentation
5. **Monitoring**: Track configuration changes and their impact
6. **Backup**: Store configuration in version control systems
7. **Testing**: Validate configuration changes in non-production environments

## Migration and Upgrades

When upgrading the application or changing configurations:

1. **Backward Compatibility**: Ensure new parameters have sensible defaults
2. **Gradual Rollout**: Test configuration changes in staging environments
3. **Rollback Plan**: Maintain previous working configuration for quick rollback
4. **Monitoring**: Increase monitoring during configuration changes
5. **Documentation**: Update configuration documentation with changes

## Support and Troubleshooting

For configuration-related issues:

1. **Check Application Logs**: Review CloudWatch Logs for startup errors
2. **Validate IAM Permissions**: Ensure all required permissions are granted
3. **Test Connectivity**: Verify network access to all services
4. **Review Defaults**: Check if default values are appropriate for your environment
5. **Consult Documentation**: Reference this guide and AWS documentation

For additional support, contact the development team with:
- Complete configuration parameters (sanitized)
- Application logs and error messages
- Environment details (region, VPC, security groups)
- Timeline of configuration changes 