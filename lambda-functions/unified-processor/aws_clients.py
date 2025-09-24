import boto3
import json
import time
import logging
from typing import List, Dict, Any, Tuple, Optional
import datetime
from config import (
    LAMBDA_REGION, KINESIS_STREAM_NAME, UNIFIED_DLQ_URL, POISON_QUEUE_URL,
    ALERT_SNS_TOPIC_ARN, METRICS_NAMESPACE, get_retry_delay, MAX_RETRY_ATTEMPTS
)

# ================================================================================
# AWS Service Clients and Utilities
# ================================================================================
"""
Simplified AWS service interactions for the unified processor:
- Kinesis operations (with simple retry logic)
- SQS operations (DLQ and Poison Queue)
- SNS operations (for alerting)
- S3 operations (for historical replay)
- CloudWatch metrics publishing
"""

# AWS Clients
kinesis = boto3.client("kinesis", region_name=LAMBDA_REGION)
sqs = boto3.client("sqs", region_name=LAMBDA_REGION)
sns = boto3.client("sns", region_name=LAMBDA_REGION)
s3 = boto3.client("s3", region_name=LAMBDA_REGION)
cloudwatch = boto3.client("cloudwatch", region_name=LAMBDA_REGION)

# Logging
logger = logging.getLogger(__name__)

# ================================================================================
# Kinesis Operations
# ================================================================================

def put_records_to_kinesis(records: List[Dict[str, Any]]) -> Tuple[bool, List[Dict[str, Any]], str]:
    """
    Send records to Kinesis with simple retry logic
    
    Args:
        records: List of Kinesis records with Data and PartitionKey
        
    Returns:
        Tuple of (success, failed_records, error_message)
    """
    logger.info(f"🚀 KINESIS: Starting put_records_to_kinesis with {len(records)} records")
    logger.info(f"🚀 KINESIS: Target stream: {KINESIS_STREAM_NAME}")
    logger.info(f"🚀 KINESIS: Region: {LAMBDA_REGION}")
    
    # Log first record for debugging
    if records:
        logger.info(f"🚀 KINESIS: First record sample: {json.dumps(records[0], indent=2, default=str)}")
    
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            logger.info(f"🚀 KINESIS: Attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS}: Sending {len(records)} records")
            
            # Log the exact payload being sent
            logger.info(f"🚀 KINESIS: Calling kinesis.put_records with StreamName={KINESIS_STREAM_NAME}")
            logger.info(f"🚀 KINESIS: Records payload: {json.dumps(records, indent=2, default=str)}")
            
            response = kinesis.put_records(
                StreamName=KINESIS_STREAM_NAME,
                Records=records
            )
            
            logger.info(f"🚀 KINESIS: Response received: {json.dumps(response, indent=2, default=str)}")
            
            failed_record_count = response.get("FailedRecordCount", 0)
            logger.info(f"🚀 KINESIS: Failed record count: {failed_record_count}")
            
            if failed_record_count == 0:
                logger.info(f"✅ KINESIS: Successfully sent all {len(records)} records to Kinesis")
                publish_metric("KinesisWriteSuccess", len(records))
                return True, [], ""
            
            # Collect failed records for retry
            failed_records = []
            for i, record_result in enumerate(response.get("Records", [])):
                if "ErrorCode" in record_result:
                    failed_records.append(records[i])
                    logger.warning(f"Record {i} failed: {record_result.get('ErrorMessage', 'Unknown error')}")
            
            successful_count = len(records) - len(failed_records)
            logger.info(f"Partial success: {successful_count} succeeded, {len(failed_records)} failed")
            
            publish_metric("KinesisWritePartialSuccess", successful_count)
            publish_metric("KinesisWriteFailures", len(failed_records))
            
            # If this is the last attempt, return the failure
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return False, failed_records, f"Failed after {MAX_RETRY_ATTEMPTS} attempts"
            
            # Prepare for retry with only failed records
            records = failed_records
            if records:
                delay = get_retry_delay(attempt)
                logger.info(f"Retrying {len(records)} failed records after {delay}s delay")
                time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Kinesis operation failed (attempt {attempt + 1}): {e}")
            
            # If this is the last attempt, return the failure
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return False, records, str(e)
            
            delay = get_retry_delay(attempt)
            logger.info(f"Retrying after error in {delay}s")
            time.sleep(delay)
    
    return False, records, "Unexpected retry loop exit"

def create_kinesis_record(data: Dict[str, Any], partition_key: str) -> Dict[str, Any]:
    """
    Create a Kinesis record from data and partition key
    
    Args:
        data: Data to send to Kinesis
        partition_key: Partition key for the record
        
    Returns:
        Kinesis record dictionary
    """
    logger.info(f"🔧 Creating Kinesis record...")
    logger.info(f"🔧 Input data type: {type(data)}")
    logger.info(f"🔧 Input data: {json.dumps(data, indent=2, default=str)}")
    logger.info(f"🔧 Input partition key: {partition_key}")
    logger.info(f"🔧 Partition key length: {len(partition_key)}")
    
    # Serialize data to JSON bytes
    json_str = json.dumps(data)
    json_bytes = json_str.encode("utf-8")
    
    # Truncate partition key if needed
    truncated_partition_key = partition_key[:256]
    
    kinesis_record = {
        "Data": json_bytes,
        "PartitionKey": truncated_partition_key
    }
    
    logger.info(f"🔧 Created Kinesis record:")
    logger.info(f"🔧   - Data length: {len(json_bytes)} bytes")
    logger.info(f"🔧   - PartitionKey: {truncated_partition_key}")
    logger.info(f"🔧   - JSON string (first 500 chars): {json_str[:500]}")
    
    return kinesis_record

# ================================================================================
# SQS Operations (DLQ and Poison Queue)
# ================================================================================

def send_to_unified_dlq(failed_data: Any, error_reason: str, original_error: str, request_id: str = "unknown") -> bool:
    """
    Send failed data to the unified DLQ
    
    Args:
        failed_data: The data that failed processing
        error_reason: Category of error
        original_error: Original error message
        
    Returns:
        True if successful
    """
    try:
        dlq_message = {
            "timestamp": datetime.datetime.utcnow().isoformat() + 'Z',
            "error_reason": error_reason,
            "original_error": str(original_error),
            "failed_data": failed_data,
            "retry_count": 0,
            "lambda_request_id": request_id
        }
        
        sqs.send_message(
            QueueUrl=UNIFIED_DLQ_URL,
            MessageBody=json.dumps(dlq_message),
            MessageAttributes={
                'ErrorReason': {'StringValue': error_reason, 'DataType': 'String'},
                'RetryCount': {'StringValue': '0', 'DataType': 'Number'},
                'Timestamp': {'StringValue': dlq_message["timestamp"], 'DataType': 'String'}
            }
        )
        
        logger.info(f"Sent failed data to unified DLQ: {error_reason}")
        publish_metric("MessagesSentToDLQ", 1)
        
        # Send immediate alert
        send_alert("DLQ Message", f"New message in DLQ: {error_reason}", "WARNING")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to send message to unified DLQ: {e}")
        publish_metric("DLQSendFailures", 1)
        return False

def send_to_poison_queue(dlq_message_data: Dict[str, Any], reason: str) -> bool:
    """
    Send permanently failed message to poison queue
    
    Args:
        dlq_message_data: Original DLQ message data
        reason: Reason for moving to poison queue
        
    Returns:
        True if successful
    """
    try:
        poison_message = {
            "timestamp": datetime.datetime.utcnow().isoformat() + 'Z',
            "poison_reason": reason,
            "original_dlq_message": dlq_message_data,
            "final_failure_timestamp": datetime.datetime.utcnow().isoformat() + 'Z'
        }
        
        sqs.send_message(
            QueueUrl=POISON_QUEUE_URL,
            MessageBody=json.dumps(poison_message),
            MessageAttributes={
                'PoisonReason': {'StringValue': reason, 'DataType': 'String'},
                'OriginalError': {'StringValue': dlq_message_data.get('error_reason', 'unknown'), 'DataType': 'String'}
            }
        )
        
        logger.info(f"Sent message to poison queue: {reason}")
        publish_metric("MessagesSentToPoison", 1)
        
        # Send critical alert
        send_alert("Poison Queue Message", f"Message moved to poison queue: {reason}", "CRITICAL")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to send message to poison queue: {e}")
        publish_metric("PoisonQueueSendFailures", 1)
        return False

def receive_from_dlq(max_messages: int = 10) -> List[Dict[str, Any]]:
    """
    Receive messages from the unified DLQ
    
    Args:
        max_messages: Maximum number of messages to receive
        
    Returns:
        List of SQS message dictionaries
    """
    try:
        response = sqs.receive_message(
            QueueUrl=UNIFIED_DLQ_URL,
            MaxNumberOfMessages=max_messages,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=20  # Long polling
        )
        
        messages = response.get('Messages', [])
        logger.info(f"Received {len(messages)} messages from unified DLQ")
        
        return messages
        
    except Exception as e:
        logger.error(f"Failed to receive messages from DLQ: {e}")
        return []

def delete_from_dlq(receipt_handle: str) -> bool:
    """
    Delete a successfully processed message from DLQ
    
    Args:
        receipt_handle: SQS receipt handle
        
    Returns:
        True if successful
    """
    try:
        sqs.delete_message(
            QueueUrl=UNIFIED_DLQ_URL,
            ReceiptHandle=receipt_handle
        )
        return True
    except Exception as e:
        logger.error(f"Failed to delete message from DLQ: {e}")
        return False

def requeue_to_dlq(message_data: Dict[str, Any], retry_count: int) -> bool:
    """
    Requeue a message back to DLQ with updated retry count
    
    Args:
        message_data: Message data to requeue
        retry_count: Updated retry count
        
    Returns:
        True if successful
    """
    try:
        message_data['retry_count'] = retry_count
        message_data['last_retry_timestamp'] = datetime.datetime.utcnow().isoformat() + 'Z'
        
        delay_seconds = get_retry_delay(retry_count - 1)
        
        sqs.send_message(
            QueueUrl=UNIFIED_DLQ_URL,
            MessageBody=json.dumps(message_data),
            DelaySeconds=min(delay_seconds, 900),  # SQS max delay is 15 minutes
            MessageAttributes={
                'RetryCount': {'StringValue': str(retry_count), 'DataType': 'Number'},
                'ErrorReason': {'StringValue': message_data.get('error_reason', 'unknown'), 'DataType': 'String'}
            }
        )
        
        logger.info(f"Requeued message to DLQ with retry count {retry_count}")
        publish_metric("MessagesRequeuedToDLQ", 1)
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to requeue message to DLQ: {e}")
        return False

# ================================================================================
# SNS Operations (Alerting)
# ================================================================================

def send_alert(subject: str, message: str, level: str = "INFO") -> bool:
    """
    Send alert via SNS
    
    Args:
        subject: Alert subject
        message: Alert message
        level: Alert level (INFO, WARNING, CRITICAL)
        
    Returns:
        True if successful
    """
    try:
        alert_message = {
            "timestamp": datetime.datetime.utcnow().isoformat() + 'Z',
            "level": level,
            "subject": subject,
            "message": message,
            "environment": LAMBDA_REGION,
            "service": "ShiftOpportunity-UnifiedProcessor"
        }
        
        sns.publish(
            TopicArn=ALERT_SNS_TOPIC_ARN,
            Subject=f"[{level}] ShiftOpportunity: {subject}",
            Message=json.dumps(alert_message, indent=2),
            MessageAttributes={
                'AlertLevel': {'StringValue': level, 'DataType': 'String'},
                'Service': {'StringValue': 'ShiftOpportunity', 'DataType': 'String'}
            }
        )
        
        logger.info(f"Sent {level} alert: {subject}")
        publish_metric("AlertsSent", 1, dimensions={'AlertLevel': level})
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")
        publish_metric("AlertSendFailures", 1)
        return False

# ================================================================================
# S3 Operations (Historical Replay)
# ================================================================================

def list_s3_files(bucket: str, prefix: str) -> List[str]:
    """
    List files in S3 bucket with given prefix
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix
        
    Returns:
        List of S3 object keys
    """
    try:
        paginator = s3.get_paginator('list_objects_v2')
        files = []
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append(obj['Key'])
        
        logger.info(f"Found {len(files)} files in s3://{bucket}/{prefix}")
        return files
        
    except Exception as e:
        logger.error(f"Failed to list S3 files: {e}")
        return []

def download_s3_file(bucket: str, key: str) -> Optional[str]:
    """
    Download and return content of S3 file
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        File content as string or None if failed
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        
        # Handle gzip compression if needed
        if key.endswith('.gz'):
            import gzip
            content = gzip.decompress(content)
        
        return content.decode('utf-8')
        
    except Exception as e:
        logger.error(f"Failed to download S3 file {key}: {e}")
        return None

# ================================================================================
# CloudWatch Metrics
# ================================================================================

def publish_metric(metric_name: str, value: float, unit: str = "Count", dimensions: Dict[str, str] = None) -> bool:
    """
    Publish custom CloudWatch metric
    
    Args:
        metric_name: Name of the metric
        value: Metric value
        unit: Metric unit
        dimensions: Additional dimensions
        
    Returns:
        True if successful
    """
    try:
        metric_data = {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.datetime.utcnow()
        }
        
        if dimensions:
            metric_data['Dimensions'] = [
                {'Name': k, 'Value': v} for k, v in dimensions.items()
            ]
        
        cloudwatch.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[metric_data]
        )
        
        return True
        
    except Exception as e:
        logger.warning(f"Failed to publish metric {metric_name}: {e}")
        return False

# ================================================================================
# Health Check and Validation
# ================================================================================

def validate_aws_connectivity() -> Dict[str, bool]:
    """
    Validate connectivity to all required AWS services
    
    Returns:
        Dictionary with service connectivity status
    """
    status = {}
    
    # Test Kinesis
    try:
        kinesis.describe_stream(StreamName=KINESIS_STREAM_NAME)
        status['kinesis'] = True
    except Exception:
        status['kinesis'] = False
    
    # Test SQS DLQ
    try:
        sqs.get_queue_attributes(QueueUrl=UNIFIED_DLQ_URL, AttributeNames=['All'])
        status['dlq'] = True
    except Exception:
        status['dlq'] = False
    
    # Test SQS Poison Queue
    try:
        sqs.get_queue_attributes(QueueUrl=POISON_QUEUE_URL, AttributeNames=['All'])
        status['poison_queue'] = True
    except Exception:
        status['poison_queue'] = False
    
    # Test SNS
    try:
        sns.get_topic_attributes(TopicArn=ALERT_SNS_TOPIC_ARN)
        status['sns_alerts'] = True
    except Exception:
        status['sns_alerts'] = False
    
    return status 