import json
import logging
import time
import gzip
import re
import boto3
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from aws_clients import list_s3_files, download_s3_file, publish_metric, send_alert
from config import (
    S3_BUCKET, S3_RAW_PREFIX, S3_RECORDS_CHUNK_SIZE, S3_TIMEOUT_BUFFER_MS,
    LAMBDA_REGION, TARGET_LAMBDA_FUNCTION, BATCH_SIZE, MIN_TIME_FOR_FILE_MS
)

# ================================================================================
# S3 Historical Replay Processing Module
# ================================================================================
"""
Process historical data stored in S3 for replay through the streaming pipeline.
Handles compressed files and batch processing with timeout management.

Features (matching standalone ShiftOpportunityServiceEvents-s3HistoricalReplay.py):
- Self-invocation for large datasets (continuation pattern)
- Timeout management to prevent lambda timeouts
- Target lambda invocation for processing
- Timestamp-based filtering
- Batch processing with proper error handling
"""

logger = logging.getLogger(__name__)

# AWS clients for S3 operations and lambda invocation
s3_client = boto3.client('s3', region_name=LAMBDA_REGION)
lambda_client = boto3.client('lambda', region_name=LAMBDA_REGION)

# Configuration constants are imported from config.py

# ================================================================================
# Main S3 Processing Function
# ================================================================================

def process_s3_replay_event(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Process S3 historical replay event with continuation pattern
    
    Args:
        event: Lambda event containing S3 replay parameters:
            - files: Specific files to process (for continuation)
            - start_timestamp: Filter start time
            - end_timestamp: Filter end time
            - is_continuation: Flag indicating continuation invocation
            - bucket: S3 bucket (optional, defaults to S3_BUCKET)
            - prefix: S3 prefix (optional, defaults to S3_RAW_PREFIX)
        context: Lambda context for timeout management
        
    Returns:
        Processing results dictionary
    """
    start_time = time.time()
    is_continuation = event.get('is_continuation', False)
    continuation_info = "CONTINUATION " if is_continuation else ""
    
    logger.info(f"=== STARTING S3 HISTORICAL DATA PROCESSING {continuation_info}===")
    
    # Log execution context info for monitoring
    try:
        remaining_time = context.get_remaining_time_in_millis()
        logger.info(f"Function: {context.function_name}, Timeout in: {remaining_time}ms")
    except:
        pass
    
    publish_metric("S3ReplayStarted", 1)
    
    # Parse replay parameters from event
    replay_params = _parse_replay_parameters(event)
    
    if not replay_params["valid"]:
        return _create_s3_response(400, f"Invalid replay parameters: {replay_params['error']}", {
            "error": replay_params["error"],
            "total_files": 0,
            "processed_files": 0,
            "total_messages": 0
        })
    
    logger.info(f"S3 Replay Parameters: {replay_params}")
    
    try:
        # Get list of files to process
        files_to_process = _get_files_to_process(replay_params, event)
        
        if not files_to_process:
            return _create_s3_response(200, "No files found to process", {
                "total_files": 0,
                "processed_files": 0,
                "total_messages": 0
            })
        
        logger.info(f"Found {len(files_to_process)} files to process")
        
        # Process files with timeout management and continuation pattern
        results = _process_files_with_timeout(files_to_process, replay_params, event, context)
        
        # Calculate final metrics
        processing_time = time.time() - start_time
        
        publish_metric("S3ReplayProcessingTime", processing_time * 1000, "Milliseconds")
        publish_metric("S3ReplayFilesProcessed", results["processed_files"])
        publish_metric("S3ReplayMessagesProcessed", results["total_messages"])
        
        # Determine response status
        completion_status = "completed"
        if results.get("continuation_triggered", False):
            completion_status = "continued (more files processing in background)"
        elif results["failed_files"] and results["processed_files"] == 0:
            completion_status = "failed"
        elif results["failed_files"]:
            completion_status = "partially completed"
        
        if results["processed_files"] == len(files_to_process):
            status_code = 200
            message = f"Successfully processed all {results['processed_files']} files"
        elif results["processed_files"] > 0:
            status_code = 206  # Partial success
            message = f"Partial success: {results['processed_files']}/{len(files_to_process)} files processed"
        else:
            status_code = 500
            message = "No files were successfully processed"
        
        logger.info(f"S3 replay {completion_status}: {message} in {processing_time:.2f}s")
        
        response_data = {
            "total_files": len(files_to_process),
            "processed_files": results["processed_files"],
            "total_messages": results["total_messages"],
            "failed_files": results["failed_files"],
            "processing_time_seconds": round(processing_time, 2),
            "continuation_triggered": results.get("continuation_triggered", False),
            "is_continuation": is_continuation
        }
        
        # Add timestamp filter info to response for audit trail
        if replay_params.get("start_time"):
            response_data["start_timestamp_filter"] = replay_params["start_time"].isoformat()
        if replay_params.get("end_time"):
            response_data["end_timestamp_filter"] = replay_params["end_time"].isoformat()
        
        return _create_s3_response(status_code, f"Historical data processing {completion_status}", response_data)
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Fatal error in S3 replay processing: {e}", exc_info=True)
        
        publish_metric("S3ReplayErrors", 1)
        publish_metric("S3ReplayProcessingTime", processing_time * 1000, "Milliseconds")
        
        # Send critical alert for unexpected errors
        send_alert("S3 Replay Error", f"Unexpected error in S3 replay: {e}", "CRITICAL")
        
        return _create_s3_response(500, f"S3 replay failed: {e}", {
            "total_files": 0,
            "processed_files": 0,
            "total_messages": 0,
            "processing_time_seconds": round(processing_time, 2)
        })

def _parse_replay_parameters(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and validate S3 replay parameters from event
    
    Args:
        event: Lambda event
        
    Returns:
        Dictionary with parsed parameters and validation results
    """
    try:
        # Default parameters
        params = {
            "bucket": event.get("bucket", S3_BUCKET),
            "prefix": event.get("prefix", S3_RAW_PREFIX),
            "start_timestamp": event.get("start_timestamp"),
            "end_timestamp": event.get("end_timestamp"),
            "target_lambda": event.get("target_lambda", TARGET_LAMBDA_FUNCTION),
            "valid": True,
            "error": None
        }
        
        # Validate bucket
        if not params["bucket"]:
            params["valid"] = False
            params["error"] = "S3 bucket is required"
            return params
        
        # Parse timestamps if provided
        if params["start_timestamp"]:
            try:
                params["start_time"] = _parse_timestamp(params["start_timestamp"])
            except ValueError as e:
                params["valid"] = False
                params["error"] = f"Invalid start_timestamp: {e}"
                return params
        else:
            params["start_time"] = None
        
        if params["end_timestamp"]:
            try:
                params["end_time"] = _parse_timestamp(params["end_timestamp"])
            except ValueError as e:
                params["valid"] = False
                params["error"] = f"Invalid end_timestamp: {e}"
                return params
        else:
            params["end_time"] = None
        
        # Validate timestamp order
        if params["start_time"] and params["end_time"] and params["start_time"] > params["end_time"]:
            params["valid"] = False
            params["error"] = "start_timestamp must be before end_timestamp"
        
        return params
        
    except Exception as e:
        return {
            "valid": False,
            "error": f"Error parsing parameters: {e}"
        }

def _get_files_to_process(replay_params: Dict[str, Any], event: Dict[str, Any]) -> List[str]:
    """
    Get list of S3 files to process based on parameters
    
    Args:
        replay_params: Parsed replay parameters
        event: Original lambda event
        
    Returns:
        List of S3 file keys to process
    """
    # Check if specific files are provided in the event (continuation case)
    if 'files' in event:
        files_to_process = event['files']
        logger.info(f"Processing specific files from event: {len(files_to_process)} files")
        
        # Apply timestamp filtering to specific files if requested
        if replay_params["start_time"] or replay_params["end_time"]:
            files_to_process = _filter_files_by_timestamp(
                files_to_process, 
                replay_params["start_time"], 
                replay_params["end_time"]
            )
            logger.info(f"After timestamp filtering: {len(files_to_process)} files")
        
        return files_to_process
    
    # Otherwise, list all .gz files in the raw folder with timestamp filtering
    files_to_process = _list_gz_files_in_s3(
        replay_params["bucket"], 
        replay_params["prefix"], 
        replay_params["start_time"], 
        replay_params["end_time"]
    )
    
    # Limit to batch size to prevent timeouts
    if len(files_to_process) > BATCH_SIZE:
        logger.info(f"Found {len(files_to_process)} files, processing first {BATCH_SIZE}")
        files_to_process = files_to_process[:BATCH_SIZE]
    
    return files_to_process

def _list_gz_files_in_s3(bucket: str, prefix: str, start_time: Optional[datetime] = None, 
                       end_time: Optional[datetime] = None) -> List[str]:
    """
    List all .gz files in the S3 bucket with the given prefix, optionally filtered by timestamp.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix to search under
        start_time: Filter start time
        end_time: Filter end time
        
    Returns:
        List of .gz file keys matching criteria
    """
    logger.info(f"Listing .gz files in s3://{bucket}/{prefix}")
    
    if start_time:
        logger.info(f"Start time filter: {start_time.isoformat()}")
    if end_time:
        logger.info(f"End time filter: {end_time.isoformat()}")
    
    gz_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Use pagination to handle buckets with large numbers of objects
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.gz'):
                    gz_files.append(key)
    
    logger.info(f"Found {len(gz_files)} total .gz files")
    
    # Apply timestamp filtering if requested
    if start_time or end_time:
        filtered_files = _filter_files_by_timestamp(gz_files, start_time, end_time)
        logger.info(f"After timestamp filtering: {len(filtered_files)} files")
        return filtered_files
    
    return gz_files

def _filter_files_by_timestamp(files: List[str], start_time: Optional[datetime], end_time: Optional[datetime]) -> List[str]:
    """
    Filter files by timestamp extracted from filename
    
    Args:
        files: List of S3 file keys
        start_time: Start time filter
        end_time: End time filter
        
    Returns:
        Filtered list of file keys
    """
    if not start_time and not end_time:
        return files
    
    filtered_files = []
    
    for file_key in files:
        file_timestamp = _extract_timestamp_from_filename(file_key)
        
        if file_timestamp is None:
            # If we can't extract timestamp, include file (don't filter out)
            filtered_files.append(file_key)
            continue
        
        # Check if file timestamp is within range
        include_file = True
        
        if start_time and file_timestamp < start_time:
            include_file = False
        
        if end_time and file_timestamp > end_time:
            include_file = False
        
        if include_file:
            filtered_files.append(file_key)
    
    return filtered_files

def _extract_timestamp_from_filename(file_key: str) -> Optional[datetime]:
    """
    Extract timestamp from S3 file key path.
    
    Expected pattern: raw/YYYY/MM/DD/HH/filename-YYYY-MM-DD-HH-MM-SS-uuid.gz
    
    Args:
        file_key: S3 object key
        
    Returns:
        Extracted timestamp or None if parsing fails
    """
    try:
        # Extract timestamp from filename using regex
        # Pattern: YYYY-MM-DD-HH-MM-SS
        pattern = r'(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})'
        match = re.search(pattern, file_key)
        
        if match:
            year, month, day, hour, minute, second = match.groups()
            timestamp = datetime(
                int(year), int(month), int(day), 
                int(hour), int(minute), int(second)
            )
            return timestamp
        else:
            logger.warning(f"Could not extract timestamp from file: {file_key}")
            return None
            
    except Exception as e:
        logger.error(f"Error extracting timestamp from {file_key}: {e}")
        return None

def _process_files_with_timeout(files: List[str], replay_params: Dict[str, Any], 
                              original_event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Process files with timeout management and continuation pattern
    
    Args:
        files: List of S3 file keys to process
        replay_params: Replay parameters
        original_event: Original lambda event for context
        context: Lambda context for timeout checking
        
    Returns:
        Processing results dictionary
    """
    processed_files = 0
    total_messages = 0
    failed_files = []
    continuation_triggered = False
    
    for i, file_key in enumerate(files):
        # Check if we have enough time to process another file
        if not _has_enough_time_for_file(context):
            remaining_files = files[i:]
            logger.warning(f"Approaching timeout! {len(remaining_files)} files remaining")
            
            # Invoke continuation lambda with remaining files
            if _invoke_continuation_lambda(remaining_files, original_event, context):
                logger.info(f"Continuation lambda invoked for {len(remaining_files)} remaining files")
                continuation_triggered = True
            else:
                logger.error("Failed to invoke continuation lambda - remaining files may be lost")
                failed_files.extend(remaining_files)
            
            break  # Exit the processing loop
        
        try:
            logger.info(f"Processing file {i+1}/{len(files)}: {file_key}")
            messages_processed = _process_single_file(file_key, replay_params)
            
            if messages_processed > 0:
                processed_files += 1
                total_messages += messages_processed
                logger.info(f"Successfully processed {messages_processed} messages from {file_key}")
            else:
                failed_files.append(file_key)
                logger.warning(f"No messages processed from {file_key}")
            
        except Exception as e:
            logger.error(f"Error processing file {file_key}: {e}")
            failed_files.append(file_key)
        
        # Log progress and remaining time for monitoring
        try:
            remaining_time = context.get_remaining_time_in_millis()
            logger.info(f"Progress: {i+1}/{len(files)} files, {remaining_time}ms remaining")
        except:
            pass
    
    return {
        "processed_files": processed_files,
        "total_messages": total_messages,
        "failed_files": failed_files,
        "continuation_triggered": continuation_triggered
    }

def _process_single_file(file_key: str, replay_params: Dict[str, Any]) -> int:
    """
    Process a single .gz file and return number of messages processed.
    
    Args:
        file_key: S3 object key
        replay_params: Replay parameters
        
    Returns:
        Number of messages successfully processed
    """
    logger.info(f"Processing file: {file_key}")
    
    # Download and decompress the file
    lines = _download_and_decompress_file(replay_params["bucket"], file_key)
    if not lines:
        logger.warning(f"No data found in file {file_key}")
        return 0
    
    # Parse each line into SNS records
    sns_records = []
    for line in lines:
        sns_record = _parse_sns_message_line(line)
        if sns_record:
            sns_records.append(sns_record)
    
    if not sns_records:
        logger.warning(f"No valid SNS records found in file {file_key}")
        return 0
    
    logger.info(f"Parsed {len(sns_records)} SNS records from {file_key}")
    
    # Process records in chunks to avoid timeout and lambda payload limits
    total_processed = 0
    for i in range(0, len(sns_records), S3_RECORDS_CHUNK_SIZE):
        chunk = sns_records[i:i + S3_RECORDS_CHUNK_SIZE]
        chunk_num = (i // S3_RECORDS_CHUNK_SIZE) + 1
        total_chunks = (len(sns_records) + S3_RECORDS_CHUNK_SIZE - 1) // S3_RECORDS_CHUNK_SIZE
        
        logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} records) from {file_key}")
        
        # Create lambda event and invoke target function
        lambda_event = _create_lambda_event(chunk)
        
        if _invoke_target_lambda(lambda_event, replay_params["target_lambda"]):
            total_processed += len(chunk)
            logger.info(f"Successfully processed chunk {chunk_num}/{total_chunks} ({len(chunk)} records)")
        else:
            logger.error(f"Failed to process chunk {chunk_num}/{total_chunks} from {file_key}")
            # Continue with next chunk even if this one fails
    
    logger.info(f"Successfully processed {total_processed}/{len(sns_records)} messages from {file_key}")
    return total_processed

def _download_and_decompress_file(bucket: str, key: str) -> List[str]:
    """
    Download and decompress a .gz file from S3, return list of lines.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        Lines from decompressed file (empty lines filtered out)
    """
    logger.info(f"Downloading and decompressing s3://{bucket}/{key}")
    
    try:
        # Download the file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        compressed_data = response['Body'].read()
        
        # Decompress the gzipped data
        decompressed_data = gzip.decompress(compressed_data).decode('utf-8')
        
        # Split into lines and filter out empty lines
        lines = [line.strip() for line in decompressed_data.split('\n') if line.strip()]
        
        logger.info(f"Decompressed {len(lines)} lines from {key}")
        return lines
        
    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}")
        return []

def _parse_sns_message_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single line containing an SNS message and extract the SNS record.
    
    Args:
        line: Single line from historical file containing JSON SNS notification
        
    Returns:
        SNS record structure or None if parsing fails
    """
    try:
        # Handle potential multiple JSON objects on a single line
        line = line.strip()
        if not line:
            return None
            
        # Try to find the first complete JSON object
        decoder = json.JSONDecoder()
        try:
            sns_notification, idx = decoder.raw_decode(line)
        except json.JSONDecodeError:
            # If that fails, try parsing the entire line
            sns_notification = json.loads(line)
        
        # Extract the SNS record part - this is what the target lambda expects
        sns_record = {
            "Type": sns_notification.get("Type"),
            "MessageId": sns_notification.get("MessageId"),
            "TopicArn": sns_notification.get("TopicArn"),
            "Subject": sns_notification.get("Subject"),
            "Message": sns_notification.get("Message"),
            "Timestamp": sns_notification.get("Timestamp"),
            "SignatureVersion": sns_notification.get("SignatureVersion", "1"),
            "Signature": sns_notification.get("Signature", "historical-replay"),
            "SigningCertUrl": sns_notification.get("SigningCertUrl", ""),
            "UnsubscribeUrl": sns_notification.get("UnsubscribeUrl", ""),
            "MessageAttributes": sns_notification.get("MessageAttributes", {})
        }
        
        return sns_record
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON line: {e}")
        logger.error(f"Problematic line (first 200 chars): {line[:200]}")
        return None
    except Exception as e:
        logger.error(f"Error parsing SNS message: {e}")
        return None

def _create_lambda_event(sns_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create the event structure that the target lambda expects.
    
    Args:
        sns_records: List of parsed SNS record structures
        
    Returns:
        Lambda event structure with Records array
    """
    records = []
    
    for sns_record in sns_records:
        if sns_record:  # Skip None records
            record = {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": f"{sns_record.get('TopicArn', '')}:historical-replay",
                "Sns": sns_record
            }
            records.append(record)
    
    return {
        "Records": records
    }

def _invoke_target_lambda(event: Dict[str, Any], target_function: str) -> bool:
    """
    Invoke the target lambda function with the event.
    
    Args:
        event: Lambda event structure with historical records
        target_function: Name of target lambda function
        
    Returns:
        True if invocation successful
    """
    try:
        logger.info(f"Invoking target lambda: {target_function} with {len(event['Records'])} records")
        
        response = lambda_client.invoke(
            FunctionName=target_function,
            InvocationType='RequestResponse',  # Synchronous to ensure processing order
            Payload=json.dumps(event)
        )
        
        # Check if invocation was successful
        if response.get('StatusCode') == 200:
            logger.info(f"Successfully invoked {target_function}")
            return True
        else:
            logger.error(f"Lambda invocation failed with status: {response.get('StatusCode')}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to invoke {target_function}: {str(e)}")
        return False

def _invoke_continuation_lambda(remaining_files: List[str], original_event: Dict[str, Any], context) -> bool:
    """
    Invoke a continuation lambda to process remaining files.
    
    Args:
        remaining_files: Files still needing processing
        original_event: Original lambda event for context
        context: Lambda context for self-reference
        
    Returns:
        True if continuation invocation successful
    """
    try:
        # Create continuation event with remaining files
        continuation_event = original_event.copy()
        continuation_event['files'] = remaining_files
        continuation_event['is_continuation'] = True
        
        logger.info(f"Invoking continuation lambda with {len(remaining_files)} remaining files")
        
        response = lambda_client.invoke(
            FunctionName=context.function_name,  # Invoke self
            InvocationType='Event',  # Async invocation for continuation
            Payload=json.dumps(continuation_event)
        )
        
        if response.get('StatusCode') == 202:  # 202 for async invocation
            logger.info(f"Successfully invoked continuation lambda")
            return True
        else:
            logger.error(f"Continuation lambda invocation failed with status: {response.get('StatusCode')}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to invoke continuation lambda: {str(e)}")
        return False

def _has_enough_time_for_file(context, min_time_ms: int = MIN_TIME_FOR_FILE_MS) -> bool:
    """
    Check if we have enough time to process another file.
    
    Args:
        context: Lambda context with remaining time information
        min_time_ms: Minimum time required to process a file
        
    Returns:
        True if enough time remains
    """
    try:
        remaining_time = context.get_remaining_time_in_millis()
        return remaining_time >= min_time_ms
    except Exception as e:
        logger.warning(f"Could not check remaining time: {e}")
        return True  # Default to true if we can't check

def _is_timeout_approaching(context, buffer_ms: int = S3_TIMEOUT_BUFFER_MS) -> bool:
    """
    Check if Lambda timeout is approaching
    
    Args:
        context: Lambda context
        buffer_ms: Buffer time in milliseconds
        
    Returns:
        True if timeout is approaching
    """
    try:
        remaining_time = context.get_remaining_time_in_millis()
        return remaining_time <= buffer_ms
    except Exception:
        return False

def _parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse timestamp string in various formats.
    
    Args:
        timestamp_str: Timestamp string to parse
        
    Returns:
        Parsed datetime object
        
    Raises:
        ValueError: If timestamp cannot be parsed
    """
    if not timestamp_str:
        raise ValueError("Empty timestamp string")
    
    # Supported formats for maximum flexibility
    formats = [
        "%Y-%m-%dT%H:%M:%S",           # 2025-06-27T07:58:54
        "%Y-%m-%dT%H:%M:%SZ",          # 2025-06-27T07:58:54Z
        "%Y-%m-%d %H:%M:%S",           # 2025-06-27 07:58:54
        "%Y-%m-%d",                    # 2025-06-27
        "%Y-%m-%dT%H:%M",              # 2025-06-27T07:58
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Could not parse timestamp: {timestamp_str}")

def _create_s3_response(status_code: int, message: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create standardized S3 response dictionary
    
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
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "processing_mode": "s3-historical-replay"
        })
    }

# ================================================================================
# Utility Functions
# ================================================================================

def get_s3_processing_stats() -> Dict[str, Any]:
    """
    Get current S3 processing statistics and configuration
    
    Returns:
        Dictionary with S3 processing stats
    """
    return {
        "mode": "s3-historical-replay",
        "default_bucket": S3_BUCKET,
        "default_prefix": S3_RAW_PREFIX,
        "chunk_size": S3_RECORDS_CHUNK_SIZE,
        "timeout_buffer_ms": S3_TIMEOUT_BUFFER_MS,
        "batch_size": BATCH_SIZE,
        "min_time_for_file_ms": MIN_TIME_FOR_FILE_MS,
        "target_lambda": TARGET_LAMBDA_FUNCTION,
        "features": [
            "self-invocation continuation pattern",
            "timeout management",
            "target lambda invocation",
            "timestamp filtering",
            "batch processing"
        ],
        "supported_formats": [".gz files with SNS notifications"],
        "status": "active"
    }