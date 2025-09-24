import json
import hashlib
import logging
import datetime
import time
from typing import List, Dict, Any, Tuple
from aws_clients import put_records_to_kinesis, create_kinesis_record, send_to_unified_dlq, publish_metric
from config import REQUIRED_SNS_FIELDS, MAX_MESSAGE_SIZE_KB

# ================================================================================
# SNS to Kinesis Processing Module
# ================================================================================
"""
Simplified SNS message processing that:
1. Validates and parses SNS messages
2. Creates envelopes with metadata
3. Sends to Kinesis with simple retry
4. Sends failures to unified DLQ with alerts
"""

logger = logging.getLogger(__name__)

# ================================================================================
# Message Processing Functions
# ================================================================================

def process_sns_event(event: Dict[str, Any], context=None) -> Dict[str, Any]:
    """
    Process SNS event and send records to Kinesis
    
    Args:
        event: Lambda event containing SNS records
        
    Returns:
        Processing results dictionary
    """
    start_time = time.time()
    
    # Get request ID from context
    request_id = getattr(context, 'aws_request_id', 'unknown') if context else 'unknown'
    
    # Log the entire event structure for debugging
    logger.info("=== SNS PROCESSOR STARTED ===")
    logger.info(f"Request ID: {request_id}")
    logger.info(f"Event type: {type(event)}")
    logger.info(f"Event keys: {list(event.keys()) if isinstance(event, dict) else 'N/A'}")
    logger.info(f"Full event structure: {json.dumps(event, indent=2, default=str)}")
    
    # Handle two types of SNS events:
    # 1. Direct SNS notification (Type=Notification)
    # 2. Lambda SNS event (Records array)
    
    if "Type" in event and event["Type"] == "Notification":
        # Direct SNS notification - treat the entire event as a single SNS record
        logger.info("📧 Processing direct SNS notification")
        records = [{"Sns": event}]  # Wrap in Records format for processing
        total_records = 1
    else:
        # Standard Lambda SNS event format
        logger.info("📧 Processing Lambda SNS event with Records")
        records = event.get("Records", [])
        total_records = len(records)
    
    logger.info(f"Processing {total_records} SNS records")
    publish_metric("SNSRecordsReceived", total_records)
    
    # Log each SNS record structure
    for i, record in enumerate(records):
        logger.info(f"SNS Record {i}: Keys = {list(record.keys()) if isinstance(record, dict) else 'N/A'}")
        logger.info(f"SNS Record {i} full structure: {json.dumps(record, indent=2, default=str)}")
    
    if total_records == 0:
        return _create_response(200, "No records to process", {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0
        })
    
    # Process all SNS records
    kinesis_records = []
    parsing_errors = []
    
    logger.info(f"Starting to process {total_records} individual SNS records...")
    
    for i, record in enumerate(records):
        try:
            logger.info(f"=== Processing SNS Record {i} ===")
            logger.info(f"Raw record: {json.dumps(record, indent=2, default=str)}")
            
            sns_record = record.get("Sns")
            if not sns_record:
                logger.error(f"Record {i}: Missing Sns data in record!")
                logger.error(f"Record keys: {list(record.keys()) if isinstance(record, dict) else 'N/A'}")
                parsing_errors.append({
                    "record_index": i,
                    "error": "Missing Sns data in record",
                    "record": record
                })
                continue
            
            logger.info(f"Record {i}: Found Sns data")
            logger.info(f"Record {i}: SNS data: {json.dumps(sns_record, indent=2, default=str)}")
            
            # Validate and process SNS message
            logger.info(f"Record {i}: Calling _process_sns_message...")
            envelope, partition_key, error = _process_sns_message(sns_record)
            
            if error:
                logger.error(f"Record {i}: Failed to process SNS message: {error}")
                logger.error(f"Record {i}: SNS record that failed: {json.dumps(sns_record, indent=2, default=str)}")
                parsing_errors.append({
                    "record_index": i,
                    "error": error,
                    "message_id": sns_record.get("MessageId", "unknown")
                })
                continue
            
            logger.info(f"Record {i}: Successfully processed SNS message")
            logger.info(f"Record {i}: Generated envelope: {json.dumps(envelope, indent=2, default=str)}")
            logger.info(f"Record {i}: Generated partition key: {partition_key}")
            
            # Create Kinesis record
            logger.info(f"Record {i}: Creating Kinesis record...")
            kinesis_record = create_kinesis_record(envelope, partition_key)
            logger.info(f"Record {i}: Created Kinesis record: {json.dumps(kinesis_record, indent=2, default=str)}")
            kinesis_records.append(kinesis_record)
            logger.info(f"Record {i}: ✅ Successfully added to kinesis_records list")
            
        except Exception as e:
            logger.error(f"Record {i}: Unexpected error processing SNS record: {e}", exc_info=True)
            logger.error(f"Record {i}: Failed record: {json.dumps(record, indent=2, default=str)}")
            parsing_errors.append({
                "record_index": i,
                "error": str(e),
                "message_id": record.get("Sns", {}).get("MessageId", "unknown")
            })
    
    # Handle parsing errors
    if parsing_errors:
        logger.warning(f"Found {len(parsing_errors)} parsing errors")
        send_to_unified_dlq(parsing_errors, "sns_parsing_error", "Failed to parse SNS records", request_id)
        publish_metric("SNSParsingErrors", len(parsing_errors))
    
    # Send successful records to Kinesis
    successful_records = 0
    failed_records = 0
    
    if kinesis_records:
        success, failed_kinesis_records, error_msg = put_records_to_kinesis(kinesis_records)
        
        if success:
            successful_records = len(kinesis_records)
            logger.info(f"Successfully sent all {successful_records} records to Kinesis")
        else:
            successful_records = len(kinesis_records) - len(failed_kinesis_records)
            failed_records = len(failed_kinesis_records)
            
            logger.error(f"Failed to send {failed_records} records to Kinesis: {error_msg}")
            
            # Send failed Kinesis records to DLQ
            send_to_unified_dlq(
                failed_kinesis_records,
                "kinesis_write_failure",
                error_msg,
                request_id
            )
    
    # Calculate metrics
    processing_time = time.time() - start_time
    success_rate = (successful_records / total_records * 100) if total_records > 0 else 0
    
    publish_metric("SNSProcessingTime", processing_time * 1000, "Milliseconds")
    publish_metric("SNSSuccessfulRecords", successful_records)
    publish_metric("SNSFailedRecords", failed_records + len(parsing_errors))
    publish_metric("SNSSuccessRate", success_rate, "Percent")
    
    # Determine response status
    total_failed = failed_records + len(parsing_errors)
    if total_failed == 0:
        status_code = 200
        message = f"Successfully processed all {successful_records} records"
    elif successful_records > 0:
        status_code = 206  # Partial success
        message = f"Partial success: {successful_records} succeeded, {total_failed} failed"
    else:
        status_code = 500
        message = f"All {total_records} records failed"
    
    logger.info(f"SNS processing completed: {message} in {processing_time:.2f}s")
    
    return _create_response(status_code, message, {
        "total_records": total_records,
        "successful_records": successful_records,
        "failed_records": total_failed,
        "parsing_errors": len(parsing_errors),
        "kinesis_failures": failed_records,
        "processing_time_seconds": round(processing_time, 2),
        "success_rate_percent": round(success_rate, 2)
    })

def _process_sns_message(sns_record: Dict[str, Any]) -> Tuple[Dict[str, Any], str, str]:
    """
    Process a single SNS message and create envelope
    
    Args:
        sns_record: SNS record data
        
    Returns:
        Tuple of (envelope, partition_key, error_message)
    """
    try:
        logger.info(f"🔍 Processing SNS message: {sns_record.get('MessageId', 'unknown')}")
        logger.info(f"🔍 SNS record keys: {list(sns_record.keys())}")
        
        # Validate required SNS fields
        logger.info(f"🔍 Validating required fields: {REQUIRED_SNS_FIELDS}")
        for field in REQUIRED_SNS_FIELDS:
            if field not in sns_record:
                logger.error(f"❌ Missing required SNS field: {field}")
                logger.error(f"❌ Available fields: {list(sns_record.keys())}")
                return None, None, f"Missing required SNS field: {field}"
        
        logger.info("✅ All required SNS fields present")
        
        message_str = sns_record["Message"]
        logger.info(f"🔍 Raw message string (first 500 chars): {message_str[:500]}")
        logger.info(f"🔍 Message string length: {len(message_str)} characters")
        
        # Check message size
        message_size_kb = len(message_str.encode('utf-8')) / 1024
        logger.info(f"🔍 Message size: {message_size_kb:.1f}KB (max {MAX_MESSAGE_SIZE_KB}KB)")
        if message_size_kb > MAX_MESSAGE_SIZE_KB:
            logger.error(f"❌ Message too large: {message_size_kb:.1f}KB")
            return None, None, f"Message too large: {message_size_kb:.1f}KB (max {MAX_MESSAGE_SIZE_KB}KB)"
        
        # Parse JSON message
        logger.info("🔍 Attempting to parse message as JSON...")
        try:
            message_json = json.loads(message_str)
            logger.info(f"✅ Successfully parsed JSON message")
            logger.info(f"🔍 Parsed message type: {type(message_json)}")
            if isinstance(message_json, dict):
                logger.info(f"🔍 Parsed message keys: {list(message_json.keys())}")
            logger.info(f"🔍 Parsed message: {json.dumps(message_json, indent=2, default=str)}")
        except json.JSONDecodeError as e:
            # If JSON parsing fails, keep as string but log warning
            logger.warning(f"⚠️ Failed to parse SNS message as JSON: {e}")
            logger.warning("⚠️ Keeping message as string")
            message_json = message_str
        
        # Create envelope with metadata
        logger.info("🔍 Creating message envelope...")
        envelope = _create_message_envelope(sns_record, message_json)
        logger.info(f"✅ Created envelope: {json.dumps(envelope, indent=2, default=str)}")
        
        # Generate partition key
        logger.info("🔍 Generating partition key...")
        partition_key = _generate_partition_key(message_json, sns_record["MessageId"])
        logger.info(f"✅ Generated partition key: {partition_key}")
        
        logger.info(f"✅ Successfully processed SNS message {sns_record.get('MessageId', 'unknown')}")
        return envelope, partition_key, None
        
    except Exception as e:
        logger.error(f"❌ Exception in _process_sns_message: {e}", exc_info=True)
        logger.error(f"❌ Failed SNS record: {json.dumps(sns_record, indent=2, default=str)}")
        return None, None, str(e)

def _create_message_envelope(sns_record: Dict[str, Any], message_data: Any) -> Dict[str, Any]:
    """
    Create message envelope with SNS metadata and business data.
    This EXACTLY matches the original ShiftOpportunityServiceEvents-sns-to-kinesis.py envelope structure.
    
    Args:
        sns_record: SNS record dictionary
        message_data: Parsed message data
        
    Returns:
        Envelope dictionary
    """
    lambda_timestamp = datetime.datetime.utcnow().isoformat() + 'Z'
    
    envelope = {
        "metadata": {
            "messageId": sns_record["MessageId"],
            "sns_timestamp": sns_record["Timestamp"],
            "topicArn": sns_record["TopicArn"],
            "subject": sns_record.get("Subject"),
            "attributes": sns_record.get("MessageAttributes"),
            "lambda_timestamp": lambda_timestamp,
            "processing_attempt": 1
        },
        "message": message_data
    }
    
    return envelope

def _generate_partition_key(message_data: Any, fallback_id: str) -> str:
    """
    Generate partition key for Kinesis record.
    This EXACTLY matches the original choose_partition_key function.
    
    Use the entityId inside the business JSON if present,
    else fall back to the SNS MessageId (always unique),
    else a short SHA-256.
    
    Args:
        message_data: Parsed business message payload
        fallback_id: SNS MessageId as fallback
        
    Returns:
        Partition key (≤128 chars for Kinesis requirement)
    """
    if isinstance(message_data, dict) and message_data.get("entityId"):
        return message_data["entityId"]
    if fallback_id:
        return fallback_id
    return hashlib.sha256(json.dumps(message_data).encode()).hexdigest()[:128]

def _create_response(status_code: int, message: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create standardized response dictionary
    
    Args:
        status_code: HTTP status code
        message: Response message
        metrics: Processing metrics
        
    Returns:
        Response dictionary
    """
    return {
        "statusCode": status_code,
        "body": json.dumps({
            "message": message,
            "metrics": metrics,
            "timestamp": datetime.datetime.utcnow().isoformat() + 'Z'
        })
    }

# ================================================================================
# Validation and Health Check Functions
# ================================================================================

def validate_sns_record(sns_record: Dict[str, Any]) -> List[str]:
    """
    Validate SNS record structure and content
    
    Args:
        sns_record: SNS record to validate
        
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    # Check required fields
    for field in REQUIRED_SNS_FIELDS:
        if field not in sns_record:
            errors.append(f"Missing required field: {field}")
        elif not sns_record[field]:
            errors.append(f"Empty required field: {field}")
    
    # Check message size if Message field exists
    if "Message" in sns_record:
        message_size_kb = len(sns_record["Message"].encode('utf-8')) / 1024
        if message_size_kb > MAX_MESSAGE_SIZE_KB:
            errors.append(f"Message too large: {message_size_kb:.1f}KB (max {MAX_MESSAGE_SIZE_KB}KB)")
    
    return errors

def get_sns_processing_stats() -> Dict[str, Any]:
    """
    Get current SNS processing statistics
    
    Returns:
        Dictionary with processing stats
    """
    return {
        "mode": "sns-to-kinesis",
        "max_message_size_kb": MAX_MESSAGE_SIZE_KB,
        "required_fields": REQUIRED_SNS_FIELDS,
        "target_stream": "ShiftOpportunityServiceEvents",
        "status": "active"
    } 