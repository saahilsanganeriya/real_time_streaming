import json
import boto3
import random
import uuid
import datetime
import logging
import os
import copy
import traceback
from decimal import Decimal

# ================================================================================
# ShiftOppSimulator Data Simulator Lambda Function
# ================================================================================
# Author: Saahil Sanganeriya
# Description:
"""
This Lambda function simulates realistic shift opportunity events for testing and 
development purposes. It generates create, update, and delete events for shift 
opportunities, maintaining state in DynamoDB and publishing events to SNS.

The simulator creates realistic test data patterns that mimic production shift 
opportunity lifecycle events including claims, unclaims, modifications, and deletions.

MAJOR IMPROVEMENTS (based on production data analysis):
- Multi-country support: US, Mexico (MEX), Australia (AUS) with proper location mapping
- Realistic opportunity types: COVER, VET, VTO (instead of TEST-SIMULATED type)
- Expanded workgroups: ICQA, SORT_CENTER_GENERAL, INBOUND_STOW, etc.
- Proper timezone mapping per country/location
- batch_id used as "TEST_SIMULATED" flag (instead of opportunity type)
- Production-realistic action patterns: ADJUST_HC, CLAIM, UNCLAIM, etc.
- Country-specific location IDs (TPA1, MEX1, BWU3, etc.)
- Realistic head count ranges (VET: 1-10, others: 1-3)
"""

# Dependencies:
"""
Upstream:
    Environment Variables:
        - TARGET_LAMBDA_FUNCTION: Target lambda to invoke (default: ShiftOpportunityServiceEvents-sns-to-kinesis)
        - SIMULATOR_REGION: AWS region for operations (default: us-east-1)
        - STATE_TABLE: DynamoDB table for state management (default: ShiftOpportunitySimulatorState)
        - SNS_TOPIC_ARN: SNS topic for event publishing

Downstream:
    AWS Services:
        - DynamoDB: Opportunity state persistence
        - Lambda: Target function invocation
        - SNS: Event publishing
    Target Lambda Function:
        - ShiftOpportunityServiceEvents-sns-to-kinesis
"""

# ================================================================================
# Configuration and Initialization
# ================================================================================

# Configuration - Environment variables with sensible defaults
LAMBDA_FUNCTION_NAME = os.getenv("TARGET_LAMBDA_FUNCTION", "ShiftOpportunityServiceEvents-sns-to-kinesis")
REGION = os.getenv("SIMULATOR_REGION", "us-east-1")
DYNAMODB_TABLE = os.getenv("STATE_TABLE", "ShiftOpportunitySimulatorState")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "arn:<partition>:sns:ap-southeast-2:123456789012:ShiftOpportunityServiceEvents")

# Logging configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Log configuration on startup for debugging
logger.info(f"Configuration: REGION={REGION}, TABLE={DYNAMODB_TABLE}, TARGET_LAMBDA={LAMBDA_FUNCTION_NAME}")

# AWS clients initialization with error handling
try:
    lambda_client = boto3.client('lambda', region_name=REGION)
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(DYNAMODB_TABLE)
    logger.info(f"Successfully initialized AWS clients for region {REGION}")
except Exception as e:
    logger.error(f"Failed to initialize AWS clients: {e}")
    raise

# ================================================================================
# Static Data for Realistic Generation
# ================================================================================

# Realistic data pools for generating diverse test scenarios based on production patterns
# Country and location mapping with realistic location IDs from production data
COUNTRY_LOCATION_MAPPING = {
    "United States of America": {
        "locations": ["TPA1", "MCO1", "ABE4", "IND4", "AVP1", "LAS6", "ATL1", "DFW1", "LAX1", "SEA1", "JFK1", "ORD1", "BOS1"],
        "timezones": ["America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "America/Indiana/Indianapolis"],
        "country_codes": ["United States of America", "USA"]  # Both formats seen in production
    },
    "MEX": {
        "locations": ["MEX1", "MEX2", "MEX3", "MEX5"],
        "timezones": ["America/Mexico_City"],
        "country_codes": ["MEX"]
    },
    "AUS": {
        "locations": ["BWU3"],
        "timezones": ["Australia/Sydney"],
        "country_codes": ["AUS", "Australia"]  # Both formats seen in production
    }
}

# Expanded workgroups based on production data patterns
WORKGROUPS = [
    "Picking", "Packing", "Stowing", "Receiving", "Shipping", "Returns", "Ship Dock",
    "Inbound Stow", "INBOUND_STOW", "Transfer Out", "Vendor Receive", 
    "ICQA", "SORT_CENTER_GENERAL", "PRINT_ON_DEMAND", "PACK_SINGLE"
]

SOURCES = ["ASSOCIATE", "MANAGER", "SYSTEM", "OPERATOR"]

# Realistic opportunity types from production data
OPPORTUNITY_TYPES = ["COVER", "VET", "VTO"]

ASSOCIATE_IDS = [str(200000000 + i) for i in range(1, 1000)]  # Pool of associate IDs
USERNAMES = ["tnguyen", "pgarcia", "jdoe", "asmith", "bjohnson", "kcarter", "mwilson", 
             "dkim", "lmartin", "sbrown", "cdavis", "hlopez", "ewhite", "jtaylor"]

# Fixed pool of 20 opportunity IDs that we'll cycle through (same across all Lambda invocations)
# This ensures consistent testing with a manageable set of opportunities
OPPORTUNITY_ID_POOL = [
    "test-opp-00001", "test-opp-00002", "test-opp-00003", "test-opp-00004", "test-opp-00005",
    "test-opp-00006", "test-opp-00007", "test-opp-00008", "test-opp-00009", "test-opp-00010",
    "test-opp-00011", "test-opp-00012", "test-opp-00013", "test-opp-00014", "test-opp-00015",
    "test-opp-00016", "test-opp-00017", "test-opp-00018", "test-opp-00019", "test-opp-00020"
]
logger.info(f"Using fixed opportunity ID pool with {len(OPPORTUNITY_ID_POOL)} IDs: {OPPORTUNITY_ID_POOL[:3]}...")
logger.info(f"Multi-country support enabled: {list(COUNTRY_LOCATION_MAPPING.keys())}")
logger.info(f"Realistic opportunity types: {OPPORTUNITY_TYPES}")
logger.info(f"Simulation flag: batch_id='TEST_SIMULATED'")

# ================================================================================
# Utility Functions
# ================================================================================

def convert_decimals_to_numbers(obj):
    """
    Convert DynamoDB Decimal objects to int/float for JSON serialization.
    
    DynamoDB stores numbers as Decimal objects which are not JSON serializable.
    This function recursively converts Decimals to appropriate Python numeric types.
    
    Args:
        obj: Object that may contain Decimal values
        
    Returns:
        Object with Decimals converted to int/float
    """
    if isinstance(obj, list):
        return [convert_decimals_to_numbers(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals_to_numbers(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        # Convert Decimal to int if it's a whole number, otherwise float
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj

# ================================================================================
# DynamoDB State Management Functions
# ================================================================================

def get_opportunity_data(opp_id):
    """
    Get full opportunity data from DynamoDB with comprehensive error handling.
    
    Retrieves stored opportunity state including sequence numbers and data payloads.
    This is essential for maintaining event sequence and generating realistic updates.
    
    Args:
        opp_id (str): Opportunity identifier
        
    Returns:
        dict: Opportunity data with converted Decimals, empty dict if not found
    """
    try:
        logger.info(f"Attempting to get opportunity data for ID: {opp_id}")
        response = table.get_item(Key={'opportunityId': opp_id})
        
        if 'Item' in response:
            logger.info(f"Found existing opportunity {opp_id} with state: {response['Item'].get('state', 'UNKNOWN')}")
            # Convert Decimals to proper types for JSON serialization
            return convert_decimals_to_numbers(response['Item'])
        else:
            logger.info(f"No existing opportunity found for ID: {opp_id}")
            return {}
            
    except Exception as e:
        logger.error(f"ERROR getting opportunity data for {opp_id}: {str(e)}")
        logger.error(f"Full error traceback: {traceback.format_exc()}")
        # Return empty dict to allow operation to continue (will create new opportunity)
        return {}

def save_opportunity_data(opp_id, state, opportunity_data, sequence_number):
    """
    Save complete opportunity data to DynamoDB with comprehensive error handling.
    
    Persists opportunity state for maintaining sequence across lambda invocations.
    Includes verification step to ensure data consistency.
    
    Args:
        opp_id (str): Opportunity identifier
        state (str): Current opportunity state (CREATED/UPDATED/DELETED)
        opportunity_data (dict): Complete opportunity payload
        sequence_number (int): Event sequence number for ordering
    """
    try:
        logger.info(f"Attempting to save opportunity {opp_id} with state: {state}, sequence: {sequence_number}")
        
        # Convert datetime objects to strings for DynamoDB compatibility
        serializable_data = json.loads(json.dumps(opportunity_data, default=str))
        
        item = {
            'opportunityId': opp_id,
            'state': state,
            'opportunityData': serializable_data,
            'sequenceNumber': sequence_number,
            'updatedAt': datetime.datetime.utcnow().isoformat()
        }
        
        response = table.put_item(Item=item)
        logger.info(f"Successfully saved opportunity {opp_id} to DynamoDB. Response: {response}")
        
        # Verify the save by immediately reading it back for data consistency
        verify_response = table.get_item(Key={'opportunityId': opp_id})
        if 'Item' in verify_response:
            logger.info(f"Verification: Successfully read back opportunity {opp_id}")
        else:
            logger.error(f"Verification FAILED: Could not read back opportunity {opp_id} after saving")
            
    except Exception as e:
        logger.error(f"ERROR saving opportunity data for {opp_id}: {str(e)}")
        logger.error(f"Full error traceback: {traceback.format_exc()}")
        # Don't raise the error - allow the lambda to continue

def get_all_opportunities():
    """
    Get all opportunities from DynamoDB with error handling.
    
    Provides overview of current simulation state for monitoring and debugging.
    
    Returns:
        list: All opportunity items in DynamoDB table
    """
    try:
        logger.info("Scanning all opportunities from DynamoDB")
        response = table.scan()
        items = response.get('Items', [])
        logger.info(f"Found {len(items)} total opportunities in DynamoDB")
        
        # Log summary statistics for monitoring
        states = {}
        for item in items:
            state = item.get('state', 'UNKNOWN')
            states[state] = states.get(state, 0) + 1
        logger.info(f"Opportunity states breakdown: {states}")
        
        return items
    except Exception as e:
        logger.error(f"ERROR scanning opportunities: {str(e)}")
        logger.error(f"Full error traceback: {traceback.format_exc()}")
        return []

def test_dynamodb_connection():
    """
    Test DynamoDB connectivity and permissions.
    
    Validates that the lambda can read/write to DynamoDB before processing events.
    Essential for production deployment validation.
    
    Returns:
        bool: True if connection test passes
    """
    try:
        logger.info(f"Testing DynamoDB connection to table: {DYNAMODB_TABLE}")
        
        # Test table exists and is accessible
        table_info = table.table_status
        logger.info(f"Table status: {table_info}")
        
        # Test write permission with a test item
        test_id = f"test-{uuid.uuid4()}"
        test_item = {
            'opportunityId': test_id,
            'state': 'TEST',
            'testData': 'connection_test',
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        
        table.put_item(Item=test_item)
        logger.info(f"Successfully wrote test item: {test_id}")
        
        # Test read permission
        response = table.get_item(Key={'opportunityId': test_id})
        if 'Item' in response:
            logger.info(f"Successfully read test item: {test_id}")
        else:
            logger.error(f"Failed to read back test item: {test_id}")
        
        # Clean up test item
        table.delete_item(Key={'opportunityId': test_id})
        logger.info(f"Successfully deleted test item: {test_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"DynamoDB connection test FAILED: {str(e)}")
        logger.error(f"Full error traceback: {traceback.format_exc()}")
        return False

# ================================================================================
# Timestamp Generation Functions
# ================================================================================

def generate_timestamps():
    """
    Generate realistic timestamps for event creation.
    
    Creates timestamps that simulate real-world timing between event occurrence
    and SNS publication with slight delays.
    
    Returns:
        dict: Event time and SNS timestamp with realistic delays
    """
    now = datetime.datetime.utcnow()
    event_time = now - datetime.timedelta(seconds=5)  # Event time 5 seconds ago
    sns_time = now  # SNS timestamp is current time
    
    return {
        'event_time': event_time.isoformat() + 'Z',
        'sns_time': sns_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'  # Millisecond precision
    }

def generate_shift_times():
    """
    Generate realistic shift start/end times.
    
    Creates future shift windows that represent actual workforce scheduling patterns.
    
    Returns:
        dict: Shift start and end timestamps
    """
    now = datetime.datetime.utcnow()
    # Shift starts in 1-14 days, lasts 8-12 hours
    shift_start = now + datetime.timedelta(days=random.randint(1, 14), hours=random.randint(6, 18))
    shift_end = shift_start + datetime.timedelta(hours=random.randint(8, 12))
    
    return {
        'start': shift_start.isoformat() + 'Z',
        'end': shift_end.isoformat() + 'Z'
    }

def generate_claim_times():
    """
    Generate realistic claim start/end times.
    
    Creates claim windows that allow associates to accept shift opportunities
    with realistic timeframes.
    
    Returns:
        dict: Claim start and end timestamps
    """
    now = datetime.datetime.utcnow()
    # Claim starts now to 1 hour ago, ends in 1-7 days
    claim_start = now - datetime.timedelta(minutes=random.randint(0, 60))
    claim_end = now + datetime.timedelta(days=random.randint(1, 7))
    
    return {
        'start': claim_start.isoformat() + 'Z',
        'end': claim_end.isoformat() + 'Z'
    }

# ================================================================================
# Event Generation Functions
# ================================================================================

def create_base_opportunity(opp_id):
    """
    Create a new opportunity with realistic data.
    
    Generates a complete shift opportunity data structure with all required fields
    and realistic values that match production data patterns. Uses multi-country
    support with proper timezone and location mapping.
    
    Args:
        opp_id (str): Opportunity identifier
        
    Returns:
        dict: Complete opportunity data structure
    """
    timestamps = generate_timestamps()
    shift_times = generate_shift_times()
    claim_times = generate_claim_times()
    
    # Select a random country and get corresponding locations/timezones
    country_key = random.choice(list(COUNTRY_LOCATION_MAPPING.keys()))
    country_config = COUNTRY_LOCATION_MAPPING[country_key]
    
    # Pick actual country code (some countries have multiple formats)
    country = random.choice(country_config["country_codes"])
    location_id = random.choice(country_config["locations"])
    timezone = random.choice(country_config["timezones"])
    
    # Generate encoded key for shiftMatchRequestDetails - matches production format
    encoded_key = "eyJhc3NvY2lhdGVJZCI6IjAwMDAwMDAwMCIsIndvcmtXZWVrIjoiMjAyNS1XMjciLCJpZCI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCJ9"
    
    # Select realistic opportunity type and creator based on type
    opportunity_type = random.choice(OPPORTUNITY_TYPES)
    
    # COVER opportunities are usually created by SYSTEM/ASSOCIATE, VET/VTO by OPERATOR
    if opportunity_type == "COVER":
        created_by = random.choice(["SYSTEM", "ASSOCIATE"])
        source = "ASSOCIATE"
    else:  # VET or VTO
        created_by = random.choice(USERNAMES)
        source = "OPERATOR"
    
    return {
        "id": opp_id,
        "timeRange": {
            "start": shift_times['start'],
            "end": shift_times['end']
        },
        "timezone": timezone,
        "locationId": location_id,
        "country": country,
        "workgroup": random.choice(WORKGROUPS),
        "source": source,
        "type": opportunity_type,
        "claimStartsAt": claim_times['start'],
        "claimEndsAt": claim_times['end'],
        "createdAt": timestamps['event_time'],
        "createdBy": created_by,
        "lastUpdatedAt": timestamps['event_time'],
        "lastUpdatedBy": created_by,
        "lastActionTaken": "CREATE",
        "batchId": "TEST_SIMULATED",  # Use batch_id as simulation flag instead of opportunity type
        "totalHeadCount": random.randint(1, 10) if opportunity_type == "VET" else random.randint(1, 3),
        "acceptedAssociateIds": [],
        "dropPenaltyStartsAt": None,
        "dropEndsAt": None,
        "shiftMatchRequestDetails": {encoded_key: None},
        "payModifiers": []
    }

def generate_opportunity_created_event(opp_id):
    """
    Generate an OpportunityCreated event.
    
    Creates a complete event structure for new opportunity creation with proper
    SNS message format and business data payload.
    
    Args:
        opp_id (str): Opportunity identifier
        
    Returns:
        tuple: (message, sns_time, opportunity_data)
    """
    timestamps = generate_timestamps()
    opportunity_data = create_base_opportunity(opp_id)
    
    message = {
        "entityId": str(uuid.uuid4()),
        "event": {
            "serializationKey": "OpportunityCreated",
            "serializedData": {
                "newOpp": opportunity_data  # Only newOpp for created events
            }
        },
        "eventTime": timestamps['event_time'],
        "sequenceNumber": 1
    }
    
    return message, timestamps['sns_time'], opportunity_data

def calculate_fields_changed(old_opp, new_opp):
    """
    Calculate which fields changed between old and new opportunity.
    
    Tracks field-level changes for audit purposes and downstream processing.
    Handles nested objects and arrays properly.
    
    Args:
        old_opp (dict): Previous opportunity state
        new_opp (dict): New opportunity state
        
    Returns:
        list: Field names that changed
    """
    fields_changed = []
    
    # Check each field for changes
    for key in new_opp.keys():
        if key in old_opp:
            if new_opp[key] != old_opp[key]:
                # Special handling for nested objects
                if key == "timeRange":
                    if new_opp[key].get("start") != old_opp[key].get("start"):
                        fields_changed.append("timeRange.start")
                    if new_opp[key].get("end") != old_opp[key].get("end"):
                        fields_changed.append("timeRange.end")
                elif key == "shiftMatchRequestDetails":
                    # Check for changes in nested dictionary
                    for nested_key in new_opp[key].keys():
                        if nested_key not in old_opp[key] or new_opp[key][nested_key] != old_opp[key][nested_key]:
                            fields_changed.append(f"shiftMatchRequestDetails.{nested_key}")
                else:
                    fields_changed.append(key)
    
    return fields_changed

def generate_opportunity_updated_event(opp_id, existing_data, sequence_number):
    """
    Generate an OpportunityUpdated event with realistic changes.
    
    Simulates common business operations like claiming, unclaiming, and modifications
    with realistic field changes that mirror production patterns.
    
    Args:
        opp_id (str): Opportunity identifier
        existing_data (dict): Current opportunity state from DynamoDB
        sequence_number (int): Next sequence number for event ordering
        
    Returns:
        tuple: (message, sns_time, new_opportunity_data, fields_changed)
    """
    timestamps = generate_timestamps()
    
    # Copy existing data for old and new states
    old_opp = copy.deepcopy(existing_data['opportunityData'])
    new_opp = copy.deepcopy(existing_data['opportunityData'])
    
    # Realistic updates based on common business actions seen in production data
    possible_actions = ["CLAIM", "UNCLAIM", "MODIFY", "EXTEND", "ADJUST_HC"]
    action = random.choice(possible_actions)
    
    # Always update these tracking fields
    new_opp["lastUpdatedAt"] = timestamps['event_time']
    
    # Use realistic updater based on opportunity type and action
    if action in ["CLAIM", "UNCLAIM"]:
        # Claims/unclaims usually done by associates or specific users
        new_opp["lastUpdatedBy"] = random.choice(USERNAMES)
    elif action == "ADJUST_HC":
        # Head count adjustments often done by SYSTEM
        new_opp["lastUpdatedBy"] = random.choice(["SYSTEM"] + USERNAMES)
    else:
        new_opp["lastUpdatedBy"] = random.choice(USERNAMES)
    
    new_opp["lastActionTaken"] = action
    
    # Action-specific changes that mirror real business operations
    if action == "CLAIM":
        associate_id = random.choice(ASSOCIATE_IDS)
        new_opp["acceptedAssociateIds"] = [associate_id]
        # Update shift match request details
        encoded_key = list(new_opp["shiftMatchRequestDetails"].keys())[0]
        new_opp["shiftMatchRequestDetails"][encoded_key] = associate_id
    elif action == "UNCLAIM":
        new_opp["acceptedAssociateIds"] = []
        # Clear shift match request details
        encoded_key = list(new_opp["shiftMatchRequestDetails"].keys())[0]
        new_opp["shiftMatchRequestDetails"][encoded_key] = None
    elif action == "ADJUST_HC":
        # Head count adjustments - very common in production data
        # Usually reduce to 0 or adjust within reasonable bounds
        current_hc = new_opp.get("totalHeadCount", 1)
        if random.random() < 0.4:  # 40% chance to set to 0 (like production patterns)
            new_opp["totalHeadCount"] = 0
        else:
            # Adjust up or down by 1-2
            adjustment = random.randint(-2, 2)
            new_hc = max(0, min(10, current_hc + adjustment))
            new_opp["totalHeadCount"] = new_hc
    elif action == "MODIFY":
        # Occasionally modify other fields (realistic business change frequencies)
        if random.random() < 0.1:
            max_hc = 10 if new_opp.get("type") == "VET" else 3
            new_opp["totalHeadCount"] = random.randint(1, max_hc)
        if random.random() < 0.05:  # Very rare to change workgroup
            new_opp["workgroup"] = random.choice(WORKGROUPS)
        if random.random() < 0.1:
            # Extend claim end time
            current_claim_end = datetime.datetime.fromisoformat(new_opp["claimEndsAt"].replace('Z', '+00:00'))
            new_claim_end = current_claim_end + datetime.timedelta(days=random.randint(1, 3))
            new_opp["claimEndsAt"] = new_claim_end.isoformat() + 'Z'
    
    # Calculate fields that changed for audit tracking
    fields_changed = calculate_fields_changed(old_opp, new_opp)
    
    message = {
        "entityId": str(uuid.uuid4()),
        "event": {
            "serializationKey": "OpportunityUpdated",
            "serializedData": {
                "oldOpp": old_opp,  # Both oldOpp and newOpp for updates
                "newOpp": new_opp
            }
        },
        "eventTime": timestamps['event_time'],
        "sequenceNumber": sequence_number
    }
    
    return message, timestamps['sns_time'], new_opp, fields_changed

def generate_opportunity_deleted_event(opp_id, existing_data, sequence_number):
    """
    Generate an OpportunityDeleted event.
    
    Creates deletion event with final opportunity state for audit purposes.
    
    Args:
        opp_id (str): Opportunity identifier
        existing_data (dict): Current opportunity state from DynamoDB
        sequence_number (int): Next sequence number for event ordering
        
    Returns:
        tuple: (message, sns_time)
    """
    timestamps = generate_timestamps()
    
    # Use the current opportunity data as the final state
    final_opp = copy.deepcopy(existing_data['opportunityData'])
    
    message = {
        "entityId": str(uuid.uuid4()),
        "event": {
            "serializationKey": "OpportunityDeleted",
            "serializedData": {
                "oldOpp": final_opp  # Only oldOpp for deleted events, no newOpp
            }
        },
        "eventTime": timestamps['event_time'],
        "sequenceNumber": sequence_number
    }
    
    return message, timestamps['sns_time']

# ================================================================================
# SNS Event Creation and Lambda Invocation
# ================================================================================

def create_sns_event(message, sns_timestamp, event_type, country, opp_type, fields_changed=None):
    """
    Create SNS event structure that matches what lambda_function.py expects.
    
    Formats the business message into proper SNS event structure with all required
    metadata attributes for downstream processing.
    
    Args:
        message (dict): Business event message
        sns_timestamp (str): SNS timestamp
        event_type (str): Type of event (OpportunityCreated/Updated/Deleted)
        country (str): Country for the opportunity
        opp_type (str): Opportunity type
        fields_changed (list, optional): Fields that changed (for updates)
        
    Returns:
        dict: Complete SNS event structure
    """
    message_id = str(uuid.uuid4())
    
    # Message attributes that downstream processing expects
    message_attributes = {
        "EventType": {
            "Type": "String",
            "Value": event_type
        },
        "Country": {
            "Type": "String", 
            "Value": country
        },
        "OpportunityType": {
            "Type": "String",
            "Value": opp_type
        }
    }
    
    # Add fields changed for update events
    if fields_changed:
        message_attributes["FieldsChanged"] = {
            "Type": "String",
            "Value": json.dumps(fields_changed)
        }
    
    # Ensure message is JSON serializable by converting any Decimal objects
    serializable_message = convert_decimals_to_numbers(message)
    
    sns_event = {
        "Records": [
            {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": f"{SNS_TOPIC_ARN}:12345678-1234-1234-1234-123456789012",
                "Sns": {
                    "Type": "Notification",
                    "MessageId": message_id,
                    "TopicArn": SNS_TOPIC_ARN,
                    "Subject": None,
                    "Message": json.dumps(serializable_message),
                    "Timestamp": sns_timestamp,
                    "SignatureVersion": "1",
                    "Signature": "dummy-signature",
                    "SigningCertUrl": f"https://sns.{REGION}.amazonaws.com/dummy-cert.pem",
                    "UnsubscribeUrl": f"https://sns.{REGION}.amazonaws.com/dummy-unsubscribe",
                    "MessageAttributes": message_attributes
                }
            }
        ]
    }
    
    return sns_event

def invoke_target_lambda(sns_event):
    """
    Invoke the target lambda function with the SNS event.
    
    Sends generated event to the downstream processing lambda for ingestion
    into the real-time streaming pipeline.
    
    Args:
        sns_event (dict): Complete SNS event structure
        
    Returns:
        bool: True if invocation successful
    """
    try:
        logger.info(f"Invoking target lambda: {LAMBDA_FUNCTION_NAME}")
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(sns_event)
        )
        logger.info(f"Successfully invoked {LAMBDA_FUNCTION_NAME}. Response: {response}")
        return True
    except Exception as e:
        logger.error(f"Failed to invoke {LAMBDA_FUNCTION_NAME}: {str(e)}")
        logger.error(f"Full error traceback: {traceback.format_exc()}")
        return False

# ================================================================================
# Main Lambda Handler
# ================================================================================

def lambda_handler(event, context):
    """
    Main lambda handler - generates and sends one realistic shift opportunity event.
    
    This function implements the core simulation logic:
    1. Tests DynamoDB connectivity
    2. Selects a random opportunity from the fixed pool
    3. Determines event type based on current state
    4. Generates appropriate event with realistic data
    5. Invokes downstream processing lambda
    
    The simulator maintains state across invocations to create realistic
    event sequences (create -> update -> update -> delete).
    
    Args:
        event (dict): Lambda event (unused)
        context: Lambda context (unused)
        
    Returns:
        dict: Response with status and event details
    """
    logger.info("=== STARTING DATA SIMULATION ===")
    
    # Test DynamoDB connection first - critical for state management
    if not test_dynamodb_connection():
        logger.error("DynamoDB connection test failed - continuing with limited functionality")
    
    # Get current state overview for monitoring
    all_opps = get_all_opportunities()
    logger.info(f"Current DynamoDB state: {len(all_opps)} total opportunities")
    
    # Pick a random opportunity ID from the fixed pool
    opp_id = random.choice(OPPORTUNITY_ID_POOL)
    logger.info(f"Selected opportunity ID: {opp_id}")
    
    # Get existing data for this opportunity to determine next action
    existing_data = get_opportunity_data(opp_id)
    
    # ============================================================================
    # Event Type Selection Logic
    # ============================================================================
    
    # Determine what type of event to generate based on current state
    if not existing_data:
        # Opportunity doesn't exist, create it
        event_type = "CREATE"
        logger.info(f"No existing data for {opp_id} - will CREATE")
        message, sns_time, opp_data = generate_opportunity_created_event(opp_id)
        
        # Create SNS event structure
        sns_event = create_sns_event(
            message, sns_time, "OpportunityCreated",
            opp_data["country"], opp_data["type"]
        )
        
        # Save state for future simulations
        save_opportunity_data(opp_id, "CREATED", opp_data, 1)
        
    elif existing_data.get('state') == 'DELETED':
        # Opportunity was deleted, create a new one with same ID (realistic business pattern)
        event_type = "CREATE"
        logger.info(f"Opportunity {opp_id} was deleted - will CREATE new one")
        message, sns_time, opp_data = generate_opportunity_created_event(opp_id)
        
        sns_event = create_sns_event(
            message, sns_time, "OpportunityCreated",
            opp_data["country"], opp_data["type"]
        )
        
        save_opportunity_data(opp_id, "CREATED", opp_data, 1)
        
    else:
        # Opportunity exists, decide whether to update or delete
        current_state = existing_data.get('state', 'UNKNOWN')
        sequence_number = int(existing_data.get('sequenceNumber', 1)) + 1
        logger.info(f"Opportunity {opp_id} exists with state {current_state}, sequence {sequence_number-1}")
        
        # 70% chance update, 30% chance delete (realistic business distribution)
        if random.random() < 0.7:
            event_type = "UPDATE"
            logger.info(f"Will UPDATE opportunity {opp_id}")
            message, sns_time, new_opp_data, fields_changed = generate_opportunity_updated_event(
                opp_id, existing_data, sequence_number
            )
            
            sns_event = create_sns_event(
                message, sns_time, "OpportunityUpdated",
                new_opp_data["country"], new_opp_data["type"], fields_changed
            )
            
            save_opportunity_data(opp_id, "UPDATED", new_opp_data, sequence_number)
            
        else:
            event_type = "DELETE"
            logger.info(f"Will DELETE opportunity {opp_id}")
            message, sns_time = generate_opportunity_deleted_event(
                opp_id, existing_data, sequence_number
            )
            
            # Get country and type from existing data for SNS attributes
            existing_opp = existing_data['opportunityData']
            sns_event = create_sns_event(
                message, sns_time, "OpportunityDeleted",
                existing_opp["country"], existing_opp["type"]
            )
            
            save_opportunity_data(opp_id, "DELETED", existing_opp, sequence_number)
    
    # ============================================================================
    # Event Publication
    # ============================================================================
    
    # Invoke target lambda to process the generated event
    success = invoke_target_lambda(sns_event)
    
    logger.info(f"=== SIMULATION COMPLETE: {event_type} event for {opp_id} ===")
    
    # Return response based on success/failure
    if success:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Generated {event_type} event",
                "opportunityId": opp_id,
                "sequenceNumber": message.get("sequenceNumber", 1),
                "eventType": event_type
            })
        }
    else:
        logger.error("Failed to send event")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to send event"})
        } 