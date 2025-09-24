package com.amazon.shift.opportunity.model;

/**
 * Shift Opportunity Event - Input Data Model
 * ==========================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Input event data model for Kinesis stream processing
 * 
 * Description:
 * This class represents a shift opportunity event as received from Amazon Kinesis Data Streams.
 * Events originate from SNS notifications, are processed by Lambda, and arrive as complex 
 * nested JSON structures containing both metadata and business data.
 * 
 * Key Features:
 * - Immutable data model with final fields for thread safety
 * - Comprehensive JSON parsing with error handling
 * - COALESCE logic to handle new/old opportunity states
 * - Derived field computation for business logic
 * - Support for three event types: Created, Updated, Deleted
 * 
 * Data Structure:
 * The event contains four main sections:
 * 1. SNS Metadata: Transport layer information (timestamps, message ID, attributes)
 * 2. Message Headers: Entity identification and sequence information  
 * 3. Opportunity Data: Current state business data (locations, timing, assignments)
 * 4. Previous State: Historical data for change tracking (updates only)
 * 
 * Event Processing Logic:
 * - Created Events: Only newOpp contains data, oldOpp is null
 * - Updated Events: newOpp contains current state, oldOpp contains previous state
 * - Deleted Events: oldOpp contains final state, newOpp is null
 * 
 * Dependencies:
 * Upstream:
 *   - Amazon SNS: ShiftOpportunityServiceEvents topic
 *   - AWS Lambda: Event transformation and enrichment
 *   - Amazon Kinesis Data Streams: Event delivery mechanism
 * 
 * Downstream:
 *   - EventDeserializationSchema: Flink deserialization interface
 *   - Transformation operators: Convert to output event formats
 * 
 * JSON Structure Example:
 * {
 *   "metadata": {
 *     "messageId": "uuid",
 *     "sns_timestamp": "2024-01-01T12:00:00Z",
 *     "lambda_timestamp": "2024-01-01T12:00:01Z",
 *     "attributes": {
 *       "EventType": {"Value": "OpportunityUpdated"},
 *       "Country": {"Value": "US"}
 *     }
 *   },
 *   "message": {
 *     "entityId": "opportunity-123",
 *     "eventTime": "2024-01-01T11:59:59Z",
 *     "event": {
 *       "serializedData": {
 *         "newOpp": {...}, // Current state
 *         "oldOpp": {...}  // Previous state (for updates)
 *       }
 *     }
 *   }
 * }
 * 
 * Performance Considerations:
 * - Uses Gson for efficient JSON parsing
 * - Immutable design prevents accidental modifications
 * - Static parsing method enables reuse across threads
 * - Error handling prevents pipeline failures from malformed data
 */

import com.google.gson.*;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.List;

/**
 * Immutable data model representing a shift opportunity event from Kinesis.
 * 
 * All fields are final to ensure thread safety and prevent accidental modifications
 * during stream processing operations.
 */
public class ShiftOpportunityEvent {
    
    // ====================================
    // SNS METADATA FIELDS
    // ====================================
    // Transport layer information from SNS message envelope
    
    /** Unique identifier for the SNS message (used for deduplication) */
    public final String messageId;
    
    /** Timestamp when the message was published to SNS topic */
    public final Instant snsTimestamp;
    
    /** Timestamp when Lambda processed the SNS message (added during transformation) */
    public final Instant lambdaTimestamp;
    
    /** ARN of the SNS topic that published this message */
    public final String topicArn;
    
    /** Type of opportunity event: OpportunityCreated, OpportunityUpdated, OpportunityDeleted */
    public final String eventType;
    
    /** Geographic country code for the opportunity (US, UK, etc.) */
    public final String country;
    
    /** Business category of the opportunity (extracted from SNS attributes) */
    public final String opportunityType;
    
    /** Comma-separated list of fields that changed (for update events) */
    public final String fieldsChanged;
    
    // ====================================
    // MESSAGE HEADERS
    // ====================================
    // Core message identification and sequencing
    
    /** Unique identifier for the business entity (same as opportunityId) */
    public final String entityId;
    
    /** Serialization key used by the source system */
    public final String serializationKey;
    
    /** Source system timestamp when the event occurred (authoritative event time) */
    public final Instant srcTimestamp;
    
    /** Sequence number for ordering events from the same entity */
    public final Long sequenceNumber;
    
    // ====================================
    // CURRENT OPPORTUNITY STATE
    // ====================================
    // Business data representing the current state of the opportunity
    // Uses COALESCE logic: newOpp for created/updated events, oldOpp for deleted events
    
    /** Unique identifier for the shift opportunity */
    public final String opportunityId;
    
    /** Identifier of the physical location where the shift occurs */
    public final String locationId;
    
    /** Workgroup or department responsible for the shift */
    public final String workgroup;
    
    /** Source system that created the opportunity */
    public final String source;
    
    /** Timezone for the shift (e.g., "America/New_York") */
    public final String timezone;
    
    /** When the actual shift work begins */
    public final Instant shiftStartTime;
    
    /** When the actual shift work ends */
    public final Instant shiftEndTime;
    
    /** When associates can start claiming the opportunity */
    public final Instant claimStartsAt;
    
    /** When claiming period ends */
    public final Instant claimEndsAt;
    
    /** When the opportunity was first created */
    public final Instant createdAt;
    
    /** User who created the opportunity */
    public final String createdBy;
    
    /** User who last modified the opportunity */
    public final String lastUpdatedBy;
    
    /** Last action taken on the opportunity */
    public final String lastActionTaken;
    
    /** Total number of associates needed for this opportunity */
    public final Integer totalHeadCount;
    
    /** List of associate IDs who have accepted the opportunity */
    public final List<String> acceptedAssociateIds;
    
    /** Batch identifier for grouped opportunity processing */
    public final String batchId;
    
    /** When drop penalty period begins (if applicable) */
    public final Instant dropPenaltyStartsAt;
    
    /** When drop penalty period ends (if applicable) */
    public final Instant dropEndsAt;
    
    // ====================================
    // PREVIOUS STATE TRACKING
    // ====================================
    // Historical data for change detection (only populated for update events)
    
    /** Previous source timestamp (for change tracking) */
    public final Instant previousSrcTimestamp;
    
    /** Previous user who modified the opportunity */
    public final String previousLastUpdatedBy;
    
    /** Previous action taken on the opportunity */
    public final String previousLastActionTaken;
    
    /** Previous total headcount requirement */
    public final Integer previousTotalHeadCount;
    
    /** Previous list of accepted associate IDs */
    public final List<String> previousAcceptedAssociateIds;
    
    // ====================================
    // COMPUTED BUSINESS FIELDS
    // ====================================
    // Derived fields calculated during event processing
    
    /** Current fill status: OPEN, PARTIALLY_FILLED, or FILLED */
    public final String fillStatus;
    
    /** Current claim status: NOT_STARTED, ACTIVE, or EXPIRED */
    public final String claimStatus;
    
    /** Whether total headcount changed in this update */
    public final Boolean headCountChanged;
    
    /** Whether accepted associates list changed in this update */
    public final Boolean acceptedAssociatesChanged;

    /**
     * Default constructor for creating empty events.
     * Used primarily for testing and error handling scenarios.
     * Initializes all fields with safe default values.
     */
    public ShiftOpportunityEvent() {
        this.messageId = "";
        this.snsTimestamp = Instant.EPOCH;
        this.lambdaTimestamp = Instant.EPOCH;
        this.topicArn = "";
        this.eventType = "";
        this.country = "";
        this.opportunityType = "";
        this.fieldsChanged = "";
        this.entityId = "";
        this.serializationKey = "";
        this.srcTimestamp = Instant.EPOCH;
        this.sequenceNumber = 0L;
        this.opportunityId = "";
        this.locationId = "";
        this.workgroup = "";
        this.source = "";
        this.timezone = "";
        this.shiftStartTime = Instant.EPOCH;
        this.shiftEndTime = Instant.EPOCH;
        this.claimStartsAt = Instant.EPOCH;
        this.claimEndsAt = Instant.EPOCH;
        this.createdAt = Instant.EPOCH;
        this.createdBy = "";
        this.lastUpdatedBy = "";
        this.lastActionTaken = "";
        this.totalHeadCount = 0;
        this.acceptedAssociateIds = null;
        this.batchId = "";
        this.dropPenaltyStartsAt = Instant.EPOCH;
        this.dropEndsAt = Instant.EPOCH;
        this.previousSrcTimestamp = Instant.EPOCH;
        this.previousLastUpdatedBy = "";
        this.previousLastActionTaken = "";
        this.previousTotalHeadCount = 0;
        this.previousAcceptedAssociateIds = null;
        this.fillStatus = "";
        this.claimStatus = "";
        this.headCountChanged = false;
        this.acceptedAssociatesChanged = false;
    }

    /**
     * Full constructor for creating shift opportunity events with all fields.
     * 
     * This constructor is used by the static parseEvent method after extracting
     * and processing data from the JSON payload. All computed fields should be
     * calculated before calling this constructor.
     * 
     * @param messageId SNS message identifier
     * @param snsTimestamp When the message was published to SNS
     * @param lambdaTimestamp When Lambda processed the message
     * @param topicArn SNS topic ARN
     * @param eventType Type of event (Created, Updated, Deleted)
     * @param country Country code
     * @param opportunityType Business category of opportunity
     * @param fieldsChanged List of changed fields (update events only)
     * @param entityId Business entity identifier
     * @param serializationKey Serialization key from source system
     * @param srcTimestamp Source system timestamp (authoritative)
     * @param sequenceNumber Event sequence number
     * @param opportunityId Unique opportunity identifier
     * @param locationId Physical location identifier
     * @param workgroup Responsible workgroup/department
     * @param source Source system name
     * @param timezone Shift timezone
     * @param shiftStartTime When shift work begins
     * @param shiftEndTime When shift work ends
     * @param claimStartsAt When claiming period begins
     * @param claimEndsAt When claiming period ends
     * @param createdAt When opportunity was created
     * @param createdBy User who created the opportunity
     * @param lastUpdatedBy User who last updated the opportunity
     * @param lastActionTaken Last action taken
     * @param totalHeadCount Required number of associates
     * @param acceptedAssociateIds List of accepted associate IDs
     * @param batchId Batch processing identifier
     * @param dropPenaltyStartsAt When drop penalty begins
     * @param dropEndsAt When drop penalty ends
     * @param previousSrcTimestamp Previous event timestamp
     * @param previousLastUpdatedBy Previous updating user
     * @param previousLastActionTaken Previous action taken
     * @param previousTotalHeadCount Previous headcount requirement
     * @param previousAcceptedAssociateIds Previous accepted associates
     * @param fillStatus Computed fill status
     * @param claimStatus Computed claim status
     * @param headCountChanged Whether headcount changed
     * @param acceptedAssociatesChanged Whether associates changed
     */
    public ShiftOpportunityEvent(
        String messageId, Instant snsTimestamp, Instant lambdaTimestamp, String topicArn, String eventType, String country,
        String opportunityType, String fieldsChanged, String entityId, String serializationKey,
        Instant srcTimestamp, Long sequenceNumber, String opportunityId, String locationId,
        String workgroup, String source, String timezone, Instant shiftStartTime,
        Instant shiftEndTime, Instant claimStartsAt, Instant claimEndsAt, Instant createdAt,
        String createdBy, String lastUpdatedBy, String lastActionTaken,
        Integer totalHeadCount, List<String> acceptedAssociateIds, String batchId,
        Instant dropPenaltyStartsAt, Instant dropEndsAt,
        Instant previousSrcTimestamp, String previousLastUpdatedBy,
        String previousLastActionTaken, Integer previousTotalHeadCount, List<String> previousAcceptedAssociateIds,
        String fillStatus, String claimStatus, Boolean headCountChanged, Boolean acceptedAssociatesChanged
    ) {
        this.messageId = messageId;
        this.snsTimestamp = snsTimestamp;
        this.lambdaTimestamp = lambdaTimestamp;
        this.topicArn = topicArn;
        this.eventType = eventType;
        this.country = country;
        this.opportunityType = opportunityType;
        this.fieldsChanged = fieldsChanged;
        this.entityId = entityId;
        this.serializationKey = serializationKey;
        this.srcTimestamp = srcTimestamp;
        this.sequenceNumber = sequenceNumber;
        this.opportunityId = opportunityId;
        this.locationId = locationId;
        this.workgroup = workgroup;
        this.source = source;
        this.timezone = timezone;
        this.shiftStartTime = shiftStartTime;
        this.shiftEndTime = shiftEndTime;
        this.claimStartsAt = claimStartsAt;
        this.claimEndsAt = claimEndsAt;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.lastUpdatedBy = lastUpdatedBy;
        this.lastActionTaken = lastActionTaken;
        this.totalHeadCount = totalHeadCount;
        this.acceptedAssociateIds = acceptedAssociateIds;
        this.batchId = batchId;
        this.dropPenaltyStartsAt = dropPenaltyStartsAt;
        this.dropEndsAt = dropEndsAt;
        this.previousSrcTimestamp = previousSrcTimestamp;
        this.previousLastUpdatedBy = previousLastUpdatedBy;
        this.previousLastActionTaken = previousLastActionTaken;
        this.previousTotalHeadCount = previousTotalHeadCount;
        this.previousAcceptedAssociateIds = previousAcceptedAssociateIds;
        this.fillStatus = fillStatus;
        this.claimStatus = claimStatus;
        this.headCountChanged = headCountChanged;
        this.acceptedAssociatesChanged = acceptedAssociatesChanged;
    }

    // ====================================
    // JSON PARSING INFRASTRUCTURE
    // ====================================
    
    /**
     * Gson instance configured for parsing shift opportunity events.
     * 
     * Configuration:
     * - Snake case field naming to match JSON structure from Lambda
     * - Custom Instant deserializer for ISO-8601 timestamp parsing
     * - Null-safe parsing to handle optional fields gracefully
     */
    private static final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .registerTypeAdapter(Instant.class, (JsonDeserializer<Instant>) (json, typeOfT, context) -> {
                if (json.isJsonNull()) return null;
                return Instant.parse(json.getAsString());
            })
            .create();

    /**
     * Parses raw bytes from Kinesis into a ShiftOpportunityEvent object.
     * 
     * This method handles the complex JSON structure containing SNS metadata,
     * message headers, and nested opportunity data. It implements COALESCE logic
     * to extract current state from either newOpp or oldOpp depending on event type.
     * 
     * JSON Processing Steps:
     * 1. Parse raw bytes into JSON object tree
     * 2. Extract SNS metadata and attributes
     * 3. Extract message headers and identifiers
     * 4. Apply COALESCE logic for opportunity data
     * 5. Compute derived business fields
     * 6. Build immutable event object
     * 
     * Event Type Handling:
     * - Created: newOpp contains data, oldOpp is null, no previous state
     * - Updated: newOpp contains current, oldOpp contains previous state
     * - Deleted: oldOpp contains final state, newOpp is null, no previous state
     * 
     * Error Handling:
     * - Returns null for unparseable events (handled by EventDeserializationSchema)
     * - Gracefully handles missing optional fields
     * - Validates required fields during parsing
     * 
     * @param event Raw bytes from Kinesis stream record
     * @return Parsed ShiftOpportunityEvent or null if parsing fails
     */
    public static ShiftOpportunityEvent parseEvent(byte[] event) {
        try {
            // ===== STEP 1: Parse JSON Structure =====
            JsonReader jsonReader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(event)));
            JsonElement jsonElement = Streams.parse(jsonReader);
            JsonObject eventObj = jsonElement.getAsJsonObject();

            // ===== STEP 2: Extract SNS Metadata =====
            JsonObject metadata = eventObj.getAsJsonObject("metadata");
            String messageId = getStringValue(metadata, "messageId");
            Instant snsTimestamp = getInstantValue(metadata, "sns_timestamp");
            Instant lambdaTimestamp = getInstantValue(metadata, "lambda_timestamp");
            String topicArn = getStringValue(metadata, "topicArn");
            
            // ===== STEP 3: Extract SNS Attributes =====
            // SNS attributes contain business metadata as key-value pairs
            JsonObject attributes = metadata.getAsJsonObject("attributes");
            String eventType = getAttributeValue(attributes, "EventType");
            String country = getAttributeValue(attributes, "Country");
            String opportunityType = getAttributeValue(attributes, "OpportunityType");
            String fieldsChanged = getAttributeValue(attributes, "FieldsChanged");

            // ===== STEP 4: Extract Message Headers =====
            JsonObject message = eventObj.getAsJsonObject("message");
            String entityId = getStringValue(message, "entityId");
            Instant srcTimestamp = getInstantValue(message, "eventTime");
            Long sequenceNumber = getLongValue(message, "sequenceNumber");

            // ===== STEP 5: Navigate to Business Data =====
            JsonObject eventDetails = message.getAsJsonObject("event");
            String serializationKey = getStringValue(eventDetails, "serializationKey");
            JsonObject serializedData = eventDetails.getAsJsonObject("serializedData");

            // ===== STEP 6: Extract Opportunity State Objects =====
            // The core business data is split between newOpp and oldOpp
            JsonObject newOpp = serializedData.getAsJsonObject("newOpp");
            JsonObject oldOpp = serializedData.getAsJsonObject("oldOpp");

            // ===== STEP 7: Apply COALESCE Logic =====
            // Extract fields that exist in both new and old opportunity objects
            // COALESCE(newOpp.field, oldOpp.field) - prefer newOpp, fall back to oldOpp
            String opportunityId = coalesceString(newOpp, oldOpp, "id");
            String locationId = coalesceString(newOpp, oldOpp, "locationId");
            String workgroup = coalesceString(newOpp, oldOpp, "workgroup");
            String source = coalesceString(newOpp, oldOpp, "source");
            String timezone = coalesceString(newOpp, oldOpp, "timezone");
            Instant createdAt = coalesceInstant(newOpp, oldOpp, "createdAt");
            String createdBy = coalesceString(newOpp, oldOpp, "createdBy");

            // Extract nested time range objects (timeRange.start, timeRange.end)
            Instant shiftStartTime = coalesceTimeRangeInstant(newOpp, oldOpp, "start");
            Instant shiftEndTime = coalesceTimeRangeInstant(newOpp, oldOpp, "end");
            Instant claimStartsAt = coalesceInstant(newOpp, oldOpp, "claimStartsAt");
            Instant claimEndsAt = coalesceInstant(newOpp, oldOpp, "claimEndsAt");

            // ===== STEP 8: Determine State Sources Based on Event Type =====
            // Event type determines which object contains current vs previous state
            boolean isDeleted = "OpportunityDeleted".equals(eventType);
            JsonObject currentStateSource = isDeleted ? oldOpp : newOpp;
            JsonObject previousStateSource = oldOpp;

            // Current state fields (from newOpp for created/updated, from oldOpp for deleted)
            String lastUpdatedBy = currentStateSource != null ? getStringValue(currentStateSource, "lastUpdatedBy") : null;
            String lastActionTaken = currentStateSource != null ? getStringValue(currentStateSource, "lastActionTaken") : null;
            Integer totalHeadCount = currentStateSource != null ? getIntegerValue(currentStateSource, "totalHeadCount") : null;
            List<String> acceptedAssociateIds = currentStateSource != null ? getStringListValue(currentStateSource, "acceptedAssociateIds") : null;
            String batchId = currentStateSource != null ? getStringValue(currentStateSource, "batchId") : null;
            Instant dropPenaltyStartsAt = currentStateSource != null ? getInstantValue(currentStateSource, "dropPenaltyStartsAt") : null;
            Instant dropEndsAt = currentStateSource != null ? getInstantValue(currentStateSource, "dropEndsAt") : null;

            // Previous state fields - for deleted events, we need the state before the deletion
            // For created/updated events, previous state comes from oldOpp
            // For deleted events, there's no "previous" state to the deletion since oldOpp IS the final state
            Instant previousSrcTimestamp = isDeleted ? null : (previousStateSource != null ? getInstantValue(previousStateSource, "lastUpdatedAt") : null);
            String previousLastUpdatedBy = isDeleted ? null : (previousStateSource != null ? getStringValue(previousStateSource, "lastUpdatedBy") : null);
            String previousLastActionTaken = isDeleted ? null : (previousStateSource != null ? getStringValue(previousStateSource, "lastActionTaken") : null);
            Integer previousTotalHeadCount = isDeleted ? null : (previousStateSource != null ? getIntegerValue(previousStateSource, "totalHeadCount") : null);
            List<String> previousAcceptedAssociateIds = isDeleted ? null : (previousStateSource != null ? getStringListValue(previousStateSource, "acceptedAssociateIds") : null);

            // Compute derived fields using current state data
            String fillStatus = computeFillStatus(totalHeadCount, acceptedAssociateIds);
            String claimStatus = computeClaimStatus(claimStartsAt, claimEndsAt);
            Boolean headCountChanged = (totalHeadCount != null && previousTotalHeadCount != null) ? 
                                     !totalHeadCount.equals(previousTotalHeadCount) : false;
            Boolean acceptedAssociatesChanged = computeAcceptedAssociatesChanged(acceptedAssociateIds, previousAcceptedAssociateIds);

            return new ShiftOpportunityEvent(
                messageId, snsTimestamp, lambdaTimestamp, topicArn, eventType, country, opportunityType, fieldsChanged,
                entityId, serializationKey, srcTimestamp, sequenceNumber, opportunityId, locationId,
                workgroup, source, timezone, shiftStartTime, shiftEndTime, claimStartsAt,
                claimEndsAt, createdAt, createdBy, lastUpdatedBy, lastActionTaken,
                totalHeadCount, acceptedAssociateIds, batchId, dropPenaltyStartsAt, dropEndsAt,
                previousSrcTimestamp, previousLastUpdatedBy, previousLastActionTaken,
                previousTotalHeadCount, previousAcceptedAssociateIds, fillStatus, claimStatus,
                headCountChanged, acceptedAssociatesChanged
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse shift opportunity event", e);
        }
    }

    // Helper methods for parsing
    private static String getStringValue(JsonObject obj, String key) {
        return obj != null && obj.has(key) && !obj.get(key).isJsonNull() ? obj.get(key).getAsString() : null;
    }

    private static Instant getInstantValue(JsonObject obj, String key) {
        if (obj != null && obj.has(key) && !obj.get(key).isJsonNull()) {
            return Instant.parse(obj.get(key).getAsString());
        }
        return null;
    }

    private static Long getLongValue(JsonObject obj, String key) {
        return obj != null && obj.has(key) && !obj.get(key).isJsonNull() ? obj.get(key).getAsLong() : null;
    }

    private static Integer getIntegerValue(JsonObject obj, String key) {
        return obj != null && obj.has(key) && !obj.get(key).isJsonNull() ? obj.get(key).getAsInt() : null;
    }

    private static List<String> getStringListValue(JsonObject obj, String key) {
        if (obj != null && obj.has(key) && !obj.get(key).isJsonNull()) {
            return gson.fromJson(obj.get(key), List.class);
        }
        return null;
    }

    private static String getAttributeValue(JsonObject attributes, String key) {
        if (attributes != null && attributes.has(key)) {
            JsonObject attr = attributes.getAsJsonObject(key);
            return attr != null && attr.has("Value") ? attr.get("Value").getAsString() : null;
        }
        return null;
    }

    private static String coalesceString(JsonObject newOpp, JsonObject oldOpp, String key) {
        String newValue = getStringValue(newOpp, key);
        return newValue != null ? newValue : getStringValue(oldOpp, key);
    }

    private static Instant coalesceInstant(JsonObject newOpp, JsonObject oldOpp, String key) {
        Instant newValue = getInstantValue(newOpp, key);
        return newValue != null ? newValue : getInstantValue(oldOpp, key);
    }

    private static Instant coalesceTimeRangeInstant(JsonObject newOpp, JsonObject oldOpp, String key) {
        Instant newValue = null;
        if (newOpp != null && newOpp.has("timeRange")) {
            JsonObject timeRange = newOpp.getAsJsonObject("timeRange");
            newValue = getInstantValue(timeRange, key);
        }
        if (newValue != null) return newValue;
        
        if (oldOpp != null && oldOpp.has("timeRange")) {
            JsonObject timeRange = oldOpp.getAsJsonObject("timeRange");
            return getInstantValue(timeRange, key);
        }
        return null;
    }

    private static String computeFillStatus(Integer totalHeadCount, List<String> acceptedAssociateIds) {
        if (totalHeadCount == null || totalHeadCount == 0) return "OPEN";
        int acceptedCount = acceptedAssociateIds != null ? acceptedAssociateIds.size() : 0;
        
        if (acceptedCount == totalHeadCount) return "FILLED";
        if (acceptedCount > 0) return "PARTIALLY_FILLED";
        return "OPEN";
    }

    private static String computeClaimStatus(Instant claimStartsAt, Instant claimEndsAt) {
        Instant now = Instant.now();
        if (claimStartsAt != null && claimStartsAt.isAfter(now)) return "NOT_STARTED";
        if (claimEndsAt != null && claimEndsAt.isBefore(now)) return "EXPIRED";
        return "ACTIVE";
    }

    private static Boolean computeAcceptedAssociatesChanged(List<String> newIds, List<String> oldIds) {
        int newCount = newIds != null ? newIds.size() : 0;
        int oldCount = oldIds != null ? oldIds.size() : 0;
        return newCount != oldCount;
    }
} 