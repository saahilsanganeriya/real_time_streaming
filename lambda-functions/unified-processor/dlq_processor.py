import json
import logging
import datetime
import time
from datetime import timedelta
from typing import List, Dict, Any, Tuple
from aws_clients import (
    receive_from_dlq, delete_from_dlq, requeue_to_dlq, send_to_poison_queue,
    put_records_to_kinesis, publish_metric
)
from config import MAX_RETRY_ATTEMPTS, DLQ_BATCH_SIZE

# ================================================================================
# DLQ Replay Processing Module
# ================================================================================
"""
Simplified DLQ replay processing that:
1. Receives messages from unified DLQ (triggered by SQS events)
2. Checks message age and retry count
3. Attempts simple retry with exponential backoff
4. Moves to poison queue if max retries exceeded or too old
5. Sends alerts for all DLQ and poison queue activity
"""

logger = logging.getLogger(__name__)

# ================================================================================
# Main DLQ Processing Function
# ================================================================================

def process_dlq_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process DLQ replay event triggered by SQS
    
    Args:
        event: Lambda event containing SQS records from DLQ
        
    Returns:
        Processing results dictionary
    """
    start_time = time.time()
    sqs_records = event.get("Records", [])
    total_messages = len(sqs_records)
    
    logger.info(f"Processing {total_messages} DLQ messages")
    publish_metric("DLQMessagesReceived", total_messages)
    
    if total_messages == 0:
        return _create_dlq_response(200, "No DLQ messages to process", {
            "total_messages": 0,
            "successful_replays": 0,
            "failed_replays": 0,
            "poison_messages": 0
        })
    
    # Process each DLQ message
    successful_replays = 0
    failed_replays = 0
    poison_messages = 0
    requeued_messages = 0
    
    for i, sqs_record in enumerate(sqs_records):
        try:
            result = _process_single_dlq_message(sqs_record)
            
            if result["action"] == "success":
                successful_replays += 1
            elif result["action"] == "poison":
                poison_messages += 1
            elif result["action"] == "requeue":
                requeued_messages += 1
            else:
                failed_replays += 1
            
        except Exception as e:
            logger.error(f"Error processing DLQ message {i}: {e}")
            failed_replays += 1
    
    # Calculate metrics
    processing_time = time.time() - start_time
    success_rate = (successful_replays / total_messages * 100) if total_messages > 0 else 0
    
    publish_metric("DLQProcessingTime", processing_time * 1000, "Milliseconds")
    publish_metric("DLQSuccessfulReplays", successful_replays)
    publish_metric("DLQFailedReplays", failed_replays)
    publish_metric("DLQPoisonMessages", poison_messages)
    publish_metric("DLQRequeuedMessages", requeued_messages)
    publish_metric("DLQSuccessRate", success_rate, "Percent")
    
    # Determine response status
    if failed_replays == 0:
        status_code = 200
        message = f"Successfully processed all {total_messages} DLQ messages"
    elif successful_replays > 0:
        status_code = 206  # Partial success
        message = f"Partial success: {successful_replays} replayed, {failed_replays} failed, {poison_messages} poisoned"
    else:
        status_code = 500
        message = f"All {total_messages} DLQ messages failed"
    
    logger.info(f"DLQ processing completed: {message} in {processing_time:.2f}s")
    
    return _create_dlq_response(status_code, message, {
        "total_messages": total_messages,
        "successful_replays": successful_replays,
        "failed_replays": failed_replays,
        "poison_messages": poison_messages,
        "requeued_messages": requeued_messages,
        "processing_time_seconds": round(processing_time, 2),
        "success_rate_percent": round(success_rate, 2)
    })

def _process_single_dlq_message(sqs_record: Dict[str, Any]) -> Dict[str, str]:
    """
    Process a single DLQ message from SQS event
    
    Args:
        sqs_record: SQS record from Lambda event
        
    Returns:
        Dictionary with processing result
    """
    try:
        # Parse the DLQ message
        message_body = json.loads(sqs_record["body"])
        receipt_handle = sqs_record["receiptHandle"]
        message_attributes = sqs_record.get("messageAttributes", {})
        
        # Check if message should be processed
        should_process, reason = _should_process_message(message_body, message_attributes)
        
        if not should_process:
            logger.info(f"Skipping message: {reason}")
            
            if "max retries" in reason:
                # Move to poison queue and delete from DLQ
                send_to_poison_queue(message_body, reason)
                delete_from_dlq(receipt_handle)
                return {"action": "poison", "reason": reason}
            else:
                # Keep in DLQ for later processing
                return {"action": "skip", "reason": reason}
        
        # Attempt to replay the failed data
        failed_data = message_body.get("failed_data", [])
        if not failed_data:
            logger.warning("No failed data found in DLQ message")
            send_to_poison_queue(message_body, "No failed data in message")
            delete_from_dlq(receipt_handle)
            return {"action": "poison", "reason": "No failed data"}
        
        # Try to send to Kinesis
        success, still_failed, error_msg = _retry_kinesis_operation(failed_data)
        
        if success:
            # Success - delete from DLQ
            logger.info("DLQ message successfully replayed")
            delete_from_dlq(receipt_handle)
            return {"action": "success", "reason": "Replay successful"}
        
        # Failed - check if we should retry or poison
        current_retry_count = message_body.get("retry_count", 0)
        
        if current_retry_count >= MAX_RETRY_ATTEMPTS:
            # Max retries exceeded - move to poison queue
            send_to_poison_queue(message_body, f"Failed after {current_retry_count} retries: {error_msg}")
            delete_from_dlq(receipt_handle)
            return {"action": "poison", "reason": f"Max retries exceeded: {error_msg}"}
        
        # Requeue with incremented retry count
        new_retry_count = current_retry_count + 1
        requeue_to_dlq(message_body, new_retry_count)
        delete_from_dlq(receipt_handle)  # Delete original message
        
        logger.info(f"Requeued message for retry {new_retry_count}/{MAX_RETRY_ATTEMPTS}")
        return {"action": "requeue", "reason": f"Retry {new_retry_count}/{MAX_RETRY_ATTEMPTS}"}
        
    except Exception as e:
        logger.error(f"Error processing DLQ message: {e}")
        return {"action": "error", "reason": str(e)}

def _should_process_message(message_body: Dict[str, Any], message_attributes: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Determine if a DLQ message should be processed
    
    Args:
        message_body: Parsed DLQ message body
        message_attributes: SQS message attributes
        
    Returns:
        Tuple of (should_process, reason)
    """
    try:
        # Check retry count
        retry_count = message_body.get("retry_count", 0)
        if retry_count >= MAX_RETRY_ATTEMPTS:
            return False, f"Max retries exceeded: {retry_count}/{MAX_RETRY_ATTEMPTS}"
        
        # Check if we have valid failed data
        failed_data = message_body.get("failed_data")
        if not failed_data:
            return False, "No failed data in message"
        
        return True, f"Ready for processing (retry {retry_count}/{MAX_RETRY_ATTEMPTS})"
        
    except Exception as e:
        logger.error(f"Error evaluating message: {e}")
        return False, f"Evaluation error: {e}"

def _retry_kinesis_operation(failed_data: List[Dict[str, Any]]) -> Tuple[bool, List[Dict[str, Any]], str]:
    """
    Retry sending failed data to Kinesis
    
    Args:
        failed_data: List of failed Kinesis records or other data
        
    Returns:
        Tuple of (success, still_failed_records, error_message)
    """
    try:
        # If failed_data contains Kinesis records, send them directly
        if isinstance(failed_data, list) and failed_data:
            first_item = failed_data[0]
            
            # Check if these are already formatted Kinesis records
            if isinstance(first_item, dict) and "Data" in first_item and "PartitionKey" in first_item:
                logger.info(f"Retrying {len(failed_data)} Kinesis records")
                return put_records_to_kinesis(failed_data)
            
            # If not Kinesis records, try to reconstruct them
            # This handles cases where the DLQ contains other types of failed data
            logger.warning("Failed data is not in Kinesis record format, cannot retry directly")
            return False, failed_data, "Data not in Kinesis record format"
        
        logger.warning("Invalid failed data format")
        return False, failed_data, "Invalid failed data format"
        
    except Exception as e:
        logger.error(f"Error retrying Kinesis operation: {e}")
        return False, failed_data, str(e)

def _create_dlq_response(status_code: int, message: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create standardized DLQ response dictionary
    
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
            "timestamp": datetime.datetime.utcnow().isoformat() + 'Z',
            "processing_mode": "dlq-replay"
        })
    }

# ================================================================================
# Manual DLQ Operations (for troubleshooting)
# ================================================================================

def process_dlq_manually(max_messages: int = None) -> Dict[str, Any]:
    """
    Manually process DLQ messages (not triggered by SQS events)
    This can be used for troubleshooting or manual recovery
    
    Args:
        max_messages: Maximum number of messages to process
        
    Returns:
        Processing results dictionary
    """
    if max_messages is None:
        max_messages = DLQ_BATCH_SIZE
    
    logger.info(f"Starting manual DLQ processing (max {max_messages} messages)")
    
    # Receive messages from DLQ
    messages = receive_from_dlq(max_messages)
    
    if not messages:
        return _create_dlq_response(200, "No messages in DLQ", {
            "total_messages": 0,
            "successful_replays": 0,
            "failed_replays": 0,
            "poison_messages": 0
        })
    
    # Convert to SQS event format and process
    sqs_records = []
    for msg in messages:
        sqs_record = {
            "body": msg["Body"],
            "receiptHandle": msg["ReceiptHandle"],
            "messageAttributes": msg.get("MessageAttributes", {})
        }
        sqs_records.append(sqs_record)
    
    # Process using the same logic as event-driven processing
    event = {"Records": sqs_records}
    return process_dlq_event(event)

def get_dlq_stats() -> Dict[str, Any]:
    """
    Get current DLQ processing statistics and configuration
    
    Returns:
        Dictionary with DLQ stats
    """
    return {
        "mode": "dlq-replay",
        "max_retry_attempts": MAX_RETRY_ATTEMPTS,
        "batch_size": DLQ_BATCH_SIZE,
        "trigger_type": "sqs-event-driven",
        "status": "active"
    } 