import json
import logging
import time
from typing import Dict, Any
from config import (
    get_processing_mode, validate_required_config, get_config_summary, 
    ProcessingMode
)
from aws_clients import validate_aws_connectivity, publish_metric, send_alert
from sns_processor import process_sns_event, get_sns_processing_stats
from dlq_processor import process_dlq_event, get_dlq_stats
from s3_processor import process_s3_replay_event, get_s3_processing_stats

# ================================================================================
# Unified ShiftOpportunity Lambda Processor
# ================================================================================
"""
Main lambda handler for the unified ShiftOpportunity processor.
Routes between three processing modes:
1. SNS to Kinesis processing (real-time events)
2. DLQ replay processing (event-driven by SQS)
3. S3 historical replay processing (manual/scheduled)

Key features:
- Single unified DLQ for all errors
- Event-driven DLQ processing (no scheduled triggers)
- Simplified error handling with immediate alerts
- Modular design with separate processors
"""

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================================================================================
# Main Lambda Handler
# ================================================================================

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Main Lambda handler - routes to appropriate processor based on event type
    
    Args:
        event: Lambda event dictionary
        context: Lambda context object
        
    Returns:
        Processing result dictionary
    """
    start_time = time.time()
    
    # Log function invocation with full details
    logger.info("🚀 === UNIFIED PROCESSOR LAMBDA STARTED ===")
    logger.info(f"🚀 Event type: {type(event)}")
    logger.info(f"🚀 Event keys: {list(event.keys()) if isinstance(event, dict) else 'N/A'}")
    logger.info(f"🚀 Lambda context: {context}")
    logger.info(f"🚀 Lambda context attributes: {dir(context)}")
    if hasattr(context, 'aws_request_id'):
        logger.info(f"🚀 Request ID: {context.aws_request_id}")
    if hasattr(context, 'function_name'):
        logger.info(f"🚀 Function name: {context.function_name}")
    logger.info(f"🚀 Full event: {json.dumps(event, indent=2, default=str)}")
    
    # Validate configuration at startup
    logger.info("🔧 Validating configuration...")
    config_validation = validate_required_config()
    logger.info(f"🔧 Config validation result: {config_validation}")
    if not config_validation["valid"]:
        error_msg = f"Configuration validation failed: {config_validation['missing_config']}"
        logger.error(f"❌ {error_msg}")
        return _create_error_response(500, error_msg, start_time)
    
    # Log configuration summary
    config_summary = get_config_summary()
    logger.info(f"Configuration: {config_summary}")
    
    try:
        # Determine processing mode from event
        logger.info("🔍 Determining processing mode from event...")
        processing_mode = get_processing_mode(event)
        logger.info(f"✅ Processing mode detected: {processing_mode.value}")
        
        # Validate AWS connectivity
        logger.info("🔍 Validating AWS connectivity...")
        connectivity_status = validate_aws_connectivity()
        logger.info(f"🔍 Connectivity status: {connectivity_status}")
        failed_services = [service for service, status in connectivity_status.items() if not status]
        
        if failed_services:
            error_msg = f"AWS connectivity failed for services: {failed_services}"
            logger.error(f"❌ {error_msg}")
            send_alert("AWS Connectivity Error", error_msg, "CRITICAL")
            return _create_error_response(503, error_msg, start_time)
        
        logger.info(f"✅ AWS connectivity verified: {connectivity_status}")
        
        # Route to appropriate processor
        logger.info(f"🚀 Routing to {processing_mode.value} processor...")
        if processing_mode == ProcessingMode.SNS_TO_KINESIS:
            logger.info("📧 Calling SNS processor...")
            result = _handle_sns_processing(event, context)
        elif processing_mode == ProcessingMode.DLQ_REPLAY:
            logger.info("🔄 Calling DLQ processor...")
            result = _handle_dlq_processing(event)
        elif processing_mode == ProcessingMode.S3_HISTORICAL_REPLAY:
            logger.info("📦 Calling S3 processor...")
            result = _handle_s3_processing(event, context)
        else:
            error_msg = f"Unknown processing mode: {processing_mode}"
            logger.error(f"❌ {error_msg}")
            return _create_error_response(400, error_msg, start_time)
        
        # Add processing metadata
        processing_time = time.time() - start_time
        result["processing_metadata"] = {
            "processing_mode": processing_mode.value,
            "total_processing_time_seconds": round(processing_time, 3),
            "lambda_request_id": context.aws_request_id if hasattr(context, 'aws_request_id') else 'unknown',
            "function_version": context.function_version if hasattr(context, 'function_version') else 'unknown'
        }
        
        # Publish overall metrics
        publish_metric("TotalProcessingTime", processing_time * 1000, "Milliseconds", 
                      dimensions={"ProcessingMode": processing_mode.value})
        publish_metric("LambdaInvocations", 1, dimensions={"ProcessingMode": processing_mode.value})
        
        # Log completion
        status_code = result.get("statusCode", 200)
        logger.info(f"Processing completed with status {status_code} in {processing_time:.3f}s")
        
        return result
        
    except Exception as e:
        processing_time = time.time() - start_time
        error_msg = f"Unexpected error in lambda handler: {e}"
        logger.error(error_msg, exc_info=True)
        
        # Send critical alert for unexpected errors
        send_alert("Lambda Handler Error", error_msg, "CRITICAL")
        
        # Publish error metrics
        publish_metric("UnexpectedErrors", 1)
        publish_metric("TotalProcessingTime", processing_time * 1000, "Milliseconds", 
                      dimensions={"ProcessingMode": "error"})
        
        return _create_error_response(500, error_msg, start_time)

def _handle_sns_processing(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Handle SNS to Kinesis processing
    
    Args:
        event: Lambda event with SNS records
        
    Returns:
        Processing result from SNS processor
    """
    logger.info("📧 === SNS PROCESSING HANDLER ===")
    logger.info(f"📧 Routing to SNS processor with event: {json.dumps(event, indent=2, default=str)}")
    publish_metric("SNSProcessingInvocations", 1)
    
    try:
        logger.info("📧 Calling process_sns_event...")
        result = process_sns_event(event, context)
        logger.info(f"📧 SNS processor returned: {json.dumps(result, indent=2, default=str)}")
        return result
    except Exception as e:
        logger.error(f"❌ Error in SNS processing: {e}", exc_info=True)
        publish_metric("SNSProcessingErrors", 1)
        raise

def _handle_dlq_processing(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle DLQ replay processing
    
    Args:
        event: Lambda event with SQS records from DLQ
        
    Returns:
        Processing result from DLQ processor
    """
    logger.info("Routing to DLQ processor")
    publish_metric("DLQProcessingInvocations", 1)
    
    try:
        return process_dlq_event(event)
    except Exception as e:
        logger.error(f"Error in DLQ processing: {e}")
        publish_metric("DLQProcessingErrors", 1)
        raise

def _handle_s3_processing(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Handle S3 historical replay processing
    
    Args:
        event: Lambda event with S3 replay parameters
        context: Lambda context for timeout management
        
    Returns:
        Processing result from S3 processor
    """
    logger.info("Routing to S3 processor")
    publish_metric("S3ProcessingInvocations", 1)
    
    try:
        return process_s3_replay_event(event, context)
    except Exception as e:
        logger.error(f"Error in S3 processing: {e}")
        publish_metric("S3ProcessingErrors", 1)
        raise

def _create_error_response(status_code: int, error_message: str, start_time: float) -> Dict[str, Any]:
    """
    Create standardized error response
    
    Args:
        status_code: HTTP status code
        error_message: Error message
        start_time: Processing start time
        
    Returns:
        Error response dictionary
    """
    processing_time = time.time() - start_time
    
    return {
        "statusCode": status_code,
        "body": json.dumps({
            "error": error_message,
            "timestamp": time.time(),
            "processing_time_seconds": round(processing_time, 3)
        })
    }

# ================================================================================
# Health Check and Information Endpoints
# ================================================================================

def get_processor_health() -> Dict[str, Any]:
    """
    Get health status of the unified processor
    
    Returns:
        Health status dictionary
    """
    try:
        # Validate configuration
        config_status = validate_required_config()
        
        # Check AWS connectivity
        connectivity_status = validate_aws_connectivity()
        
        # Get processor statistics
        sns_stats = get_sns_processing_stats()
        dlq_stats = get_dlq_stats()
        s3_stats = get_s3_processing_stats()
        
        overall_health = (
            config_status["valid"] and 
            all(connectivity_status.values())
        )
        
        return {
            "status": "healthy" if overall_health else "unhealthy",
            "timestamp": time.time(),
            "configuration": {
                "valid": config_status["valid"],
                "missing_config": config_status.get("missing_config", [])
            },
            "aws_connectivity": connectivity_status,
            "processors": {
                "sns": sns_stats,
                "dlq": dlq_stats,
                "s3": s3_stats
            },
            "config_summary": get_config_summary()
        }
        
    except Exception as e:
        logger.error(f"Error getting processor health: {e}")
        return {
            "status": "error",
            "timestamp": time.time(),
            "error": str(e)
        }

def handle_health_check_event(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Handle health check Lambda event (can be triggered manually or by monitoring)
    
    Args:
        event: Lambda event
        context: Lambda context
        
    Returns:
        Health check response
    """
    logger.info("Processing health check request")
    
    health_status = get_processor_health()
    status_code = 200 if health_status["status"] == "healthy" else 503
    
    return {
        "statusCode": status_code,
        "body": json.dumps(health_status),
        "headers": {
            "Content-Type": "application/json"
        }
    }

# ================================================================================
# Manual Operations (for troubleshooting)
# ================================================================================

def handle_manual_operation(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Handle manual operations triggered via Lambda event
    
    Supported operations:
    - health_check: Get processor health status
    - dlq_manual_replay: Manually process DLQ messages
    - config_check: Validate configuration
    
    Args:
        event: Lambda event with operation parameters
        context: Lambda context
        
    Returns:
        Operation result
    """
    operation = event.get("operation", "health_check")
    logger.info(f"Processing manual operation: {operation}")
    
    try:
        if operation == "health_check":
            return handle_health_check_event(event, context)
        
        elif operation == "dlq_manual_replay":
            from dlq_processor import process_dlq_manually
            max_messages = event.get("max_messages", 10)
            result = process_dlq_manually(max_messages)
            return result
        
        elif operation == "config_check":
            config_status = validate_required_config()
            connectivity_status = validate_aws_connectivity()
            
            return {
                "statusCode": 200 if config_status["valid"] else 400,
                "body": json.dumps({
                    "config_validation": config_status,
                    "aws_connectivity": connectivity_status,
                    "config_summary": get_config_summary()
                })
            }
        
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": f"Unknown operation: {operation}",
                    "supported_operations": ["health_check", "dlq_manual_replay", "config_check"]
                })
            }
    
    except Exception as e:
        logger.error(f"Error in manual operation {operation}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "operation": operation
            })
        }

# ================================================================================
# Entry Point - Route based on event characteristics
# ================================================================================

# The main lambda_handler is the entry point
# Additional handlers are available for specific use cases:
# - handle_health_check_event: For health monitoring
# - handle_manual_operation: For manual troubleshooting operations 