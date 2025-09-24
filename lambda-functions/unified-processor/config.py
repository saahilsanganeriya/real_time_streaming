import os
from enum import Enum
from typing import Dict, Any

# ================================================================================
# Unified ShiftOpportunity Lambda Processor Configuration
# ================================================================================
"""
Simplified configuration for the unified lambda function that handles:
1. SNS to Kinesis processing
2. DLQ replay processing  
3. S3 historical data replay

Key Simplifications:
- Single unified DLQ for all errors
- Event-driven DLQ processing (no scheduled triggers)
- Simple exponential backoff (no circuit breakers)
- Clear age and retry limits
"""

class ProcessingMode(Enum):
    """Processing modes for the unified lambda"""
    SNS_TO_KINESIS = "sns-to-kinesis"
    DLQ_REPLAY = "dlq-replay"
    S3_HISTORICAL_REPLAY = "s3-historical-replay"

class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "INFO"
    WARNING = "WARNING" 
    CRITICAL = "CRITICAL"

# ================================================================================
# Core Configuration
# ================================================================================

# AWS Region and Service Names
LAMBDA_REGION = os.getenv("LAMBDA_REGION", "us-east-1")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "ShiftOpportunityServiceEvents")
ENVIRONMENT = os.getenv("ENVIRONMENT", "hda-gamma")

# Queue Configuration
UNIFIED_DLQ_URL = os.getenv("UNIFIED_DLQ_URL")  # Required
POISON_QUEUE_URL = os.getenv("POISON_QUEUE_URL")  # Required
ALERT_SNS_TOPIC_ARN = os.getenv("ALERT_SNS_TOPIC_ARN")  # Required

# S3 Configuration for Historical Replay
S3_BUCKET = os.getenv("S3_BUCKET", "shift-opportunity-service-events")
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw/")
TARGET_LAMBDA_FUNCTION = os.getenv("TARGET_LAMBDA_FUNCTION", "ShiftOpportunityServiceEvents-sns-to-kinesis")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "25"))
MIN_TIME_FOR_FILE_MS = int(os.getenv("MIN_TIME_FOR_FILE_MS", "30000"))

# ================================================================================
# Simplified Error Handling Configuration
# ================================================================================

# DLQ and Retry Settings
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
POISON_QUEUE_RETENTION_DAYS = int(os.getenv("POISON_QUEUE_RETENTION_DAYS", "14"))

# Exponential Backoff Settings (in seconds)
RETRY_DELAYS = [
    60,    # 1 minute for first retry
    120,   # 2 minutes for second retry  
    240    # 4 minutes for third retry
]

# Batch Processing Settings
DLQ_BATCH_SIZE = int(os.getenv("DLQ_BATCH_SIZE", "10"))
S3_RECORDS_CHUNK_SIZE = int(os.getenv("S3_RECORDS_CHUNK_SIZE", "500"))
S3_TIMEOUT_BUFFER_MS = int(os.getenv("S3_TIMEOUT_BUFFER_MS", "120000"))  # 2 minutes

# ================================================================================
# Monitoring and Alerting Configuration
# ================================================================================

# CloudWatch Metrics Namespace
METRICS_NAMESPACE = "ShiftOpportunity/UnifiedProcessor"

# Alert Thresholds
ERROR_RATE_THRESHOLD = float(os.getenv("ERROR_RATE_THRESHOLD", "5.0"))  # 5%
DLQ_MESSAGE_ALERT_THRESHOLD = int(os.getenv("DLQ_MESSAGE_ALERT_THRESHOLD", "1"))
POISON_QUEUE_ALERT_THRESHOLD = int(os.getenv("POISON_QUEUE_ALERT_THRESHOLD", "1"))

# ================================================================================
# Message Processing Configuration
# ================================================================================

# Message Validation
REQUIRED_SNS_FIELDS = ["MessageId", "Message", "Timestamp", "TopicArn"]
MAX_MESSAGE_SIZE_KB = int(os.getenv("MAX_MESSAGE_SIZE_KB", "256"))

# Kinesis Configuration
KINESIS_MAX_RETRIES = int(os.getenv("KINESIS_MAX_RETRIES", "3"))
KINESIS_RETRY_DELAY_BASE = float(os.getenv("KINESIS_RETRY_DELAY_BASE", "1.0"))

# ================================================================================
# Helper Functions
# ================================================================================

def validate_required_config() -> Dict[str, Any]:
    """
    Validate that all required configuration is present
    
    Returns:
        Dict with validation results
    """
    missing_config = []
    
    if not UNIFIED_DLQ_URL:
        missing_config.append("UNIFIED_DLQ_URL")
    if not POISON_QUEUE_URL:
        missing_config.append("POISON_QUEUE_URL") 
    if not ALERT_SNS_TOPIC_ARN:
        missing_config.append("ALERT_SNS_TOPIC_ARN")
    
    return {
        "valid": len(missing_config) == 0,
        "missing_config": missing_config
    }

def get_retry_delay(attempt: int) -> int:
    """
    Get retry delay for given attempt number
    
    Args:
        attempt: Retry attempt number (0-based)
        
    Returns:
        Delay in seconds
    """
    if attempt >= len(RETRY_DELAYS):
        return RETRY_DELAYS[-1]  # Use last delay for any additional attempts
    return RETRY_DELAYS[attempt]

def get_processing_mode(event: Dict[str, Any]) -> ProcessingMode:
    """
    Determine processing mode from Lambda event
    
    Args:
        event: Lambda event dictionary
        
    Returns:
        ProcessingMode enum value
    """
    import logging
    import json
    
    logger = logging.getLogger(__name__)
    
    logger.info(f"🔍 MODE DETECTION: Analyzing event to determine processing mode")
    logger.info(f"🔍 MODE DETECTION: Event type: {type(event)}")
    logger.info(f"🔍 MODE DETECTION: Event keys: {list(event.keys()) if isinstance(event, dict) else 'N/A'}")
    logger.info(f"🔍 MODE DETECTION: Full event: {json.dumps(event, indent=2, default=str)}")
    
    # Check for direct SNS notification (not wrapped in Lambda event)
    if "Type" in event and event["Type"] == "Notification" and "TopicArn" in event:
        logger.info(f"✅ MODE DETECTION: Detected direct SNS notification -> SNS_TO_KINESIS mode")
        return ProcessingMode.SNS_TO_KINESIS
    
    # Check if triggered by SQS (DLQ replay)
    if "Records" in event and event["Records"]:
        logger.info(f"🔍 MODE DETECTION: Found Records array with {len(event['Records'])} records")
        
        first_record = event["Records"][0]
        logger.info(f"🔍 MODE DETECTION: First record: {json.dumps(first_record, indent=2, default=str)}")
        
        event_source = first_record.get("eventSource")
        logger.info(f"🔍 MODE DETECTION: Event source: {event_source}")
        
        if event_source == "aws:sqs":
            logger.info(f"✅ MODE DETECTION: Detected SQS event source -> DLQ_REPLAY mode")
            return ProcessingMode.DLQ_REPLAY
        elif event_source == "aws:sns":
            logger.info(f"✅ MODE DETECTION: Detected SNS event source -> SNS_TO_KINESIS mode")
            return ProcessingMode.SNS_TO_KINESIS
        else:
            logger.warning(f"⚠️ MODE DETECTION: Unknown event source: {event_source}")
    
    # Check for S3 historical replay parameters
    if "mode" in event and event["mode"] == "s3-historical-replay":
        logger.info(f"✅ MODE DETECTION: Found mode='s3-historical-replay' -> S3_HISTORICAL_REPLAY mode")
        return ProcessingMode.S3_HISTORICAL_REPLAY
    
    # Default to SNS processing
    logger.info(f"✅ MODE DETECTION: No specific pattern matched -> defaulting to SNS_TO_KINESIS mode")
    return ProcessingMode.SNS_TO_KINESIS

# ================================================================================
# Configuration Summary for Logging
# ================================================================================

def get_config_summary() -> Dict[str, Any]:
    """Get configuration summary for logging and debugging"""
    return {
        "lambda_region": LAMBDA_REGION,
        "kinesis_stream": KINESIS_STREAM_NAME,
        "environment": ENVIRONMENT,
        "max_retry_attempts": MAX_RETRY_ATTEMPTS,
        "retry_delays": RETRY_DELAYS,
        "dlq_batch_size": DLQ_BATCH_SIZE,
        "error_rate_threshold": ERROR_RATE_THRESHOLD,
        "metrics_namespace": METRICS_NAMESPACE
    } 