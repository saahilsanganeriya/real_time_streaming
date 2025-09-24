/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.shift.opportunity.model;

/**
 * Audit Log Event Model for OpenSearch Indexing
 * ==============================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Complete audit trail document model for compliance and debugging
 * 
 * Description:
 * This class represents a complete audit log entry for every shift opportunity event
 * that flows through the system. It preserves the full event history including
 * metadata, change tracking, and previous state information for compliance,
 * debugging, and analytical purposes.
 * 
 * Key Features:
 * - Complete event preservation for audit trail
 * - Change tracking with previous state information
 * - SNS metadata preservation for debugging
 * - Field-level change detection
 * - Immutable design for data integrity
 * - Optimized field ordering for readability
 * 
 * OpenSearch Index Pattern:
 * - Index: shift-opportunity-events-log
 * - Pattern: Append-only (no primary key)
 * - Update Strategy: Insert only, no updates
 * - Use Case: Audit trail, compliance, debugging, analytics
 * 
 * Change Tracking Features:
 * - fields_changed: Comma-separated list of modified fields
 * - head_count_changed: Boolean flag for headcount modifications
 * - accepted_associates_changed: Boolean flag for associate list changes
 * - Previous state preservation for detailed change analysis
 * 
 * Event Metadata Preserved:
 * - SNS message ID for deduplication
 * - Topic ARN for source identification
 * - Sequence number for ordering
 * - Entity ID for correlation
 * - Serialization key for debugging
 * 
 * Field Organization:
 * 1. Core Identifiers: Business keys and event classification
 * 2. Event Metadata: SNS transport layer information
 * 3. Business Data: Current state information
 * 4. Change Detection: Boolean flags for modifications
 * 5. Previous State: Historical data for comparison
 * 6. Audit Trail: Complete timestamp chain
 * 7. Performance Metrics: Latency measurements
 * 
 * Dependencies:
 * Upstream:
 *   - ShiftOpportunityToLogEvent: Transformation operator
 *   - ShiftOpportunityEvent: Source data model from Kinesis
 * 
 * Downstream:
 *   - OpenSearch events log index: Append-only operations
 *   - Compliance queries: Audit trail analysis
 *   - Debugging tools: Error investigation and troubleshooting
 *   - Analytics pipelines: Historical trend analysis
 * 
 * JSON Output Example:
 * {
 *   "event_type": "OpportunityUpdated",
 *   "opportunity_id": "opp-123",
 *   "message_id": "sns-msg-456",
 *   "fields_changed": "totalHeadCount,acceptedAssociateIds",
 *   "total_head_count": 5,
 *   "previous_total_head_count": 3,
 *   "head_count_changed": true,
 *   "accepted_associates_changed": true,
 *   "src_timestamp": "2024-01-01T12:00:00Z",
 *   "sns_timestamp": "2024-01-01T12:00:01Z"
 * }
 * 
 * Compliance Considerations:
 * - Immutable audit trail for regulatory requirements
 * - Complete change history preservation
 * - Event sourcing capabilities for reconstruction
 * - Long-term retention through S3 backup integration
 * 
 * Performance Considerations:
 * - Append-only pattern for high write throughput
 * - No update conflicts or conditional logic
 * - Efficient time-based queries with timestamp indexing
 * - Optimized for write-heavy, read-light access patterns
 */

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.time.Instant;
import java.util.List;

/**
 * Immutable audit log data model for complete event history tracking.
 * 
 * Preserves comprehensive event information including metadata, change tracking,
 * and previous state for compliance, debugging, and analytical use cases.
 */
@JsonPropertyOrder({
    // Primary identifiers first
    "eventType", "opportunityId", "opportunityType", 
    // Event metadata
    "messageId", "topicArn", "fieldsChanged", "entityId", "serializationKey", "sequenceNumber",
    // Location and metadata
    "country", "locationId", "workgroup", "source", "timezone", "batchId",
    // Claim fields grouped
    "claimStartsAt", "claimEndsAt", "claimStatus",
    // Shift timing
    "shiftStartTime", "shiftEndTime", 
    // Creation info
    "createdAt", "createdBy",
    // Current state
    "lastUpdatedBy", "lastActionTaken", "totalHeadCount", "acceptedAssociateIds", "fillStatus",
    // Optional fields
    "dropPenaltyStartsAt", "dropEndsAt",
    // Change tracking
    "headCountChanged", "acceptedAssociatesChanged",
    // Previous state fields (second half as requested)
    "previousSrcTimestamp", "previousLastUpdatedBy", "previousLastActionTaken", 
    "previousTotalHeadCount", "previousAcceptedAssociateIds",
    // Timestamps in chronological order
    "srcTimestamp", "snsTimestamp", "lambdaTimestamp", "flinkTimestamp", 
    // Latency metrics at the end
    "endToEndLatencyMs", "snsToFlinkLatencyMs"
})
public class ShiftOpportunityLogEvent extends Document {
    // Primary identifiers (ordered for user-friendliness)
    public final String eventType;
    public final String opportunityId;
    public final String opportunityType;
    
    // Event metadata
    public final String messageId;
    public final String topicArn;
    public final String fieldsChanged;
    public final String entityId;
    public final String serializationKey;
    public final Long sequenceNumber;
    
    // Location and metadata
    public final String country;
    public final String locationId;
    public final String workgroup;
    public final String source;
    public final String timezone;
    public final String batchId;
    
    // Claim fields (grouped together)
    public final Instant claimStartsAt;
    public final Instant claimEndsAt;
    public final String claimStatus;
    
    // Shift timing
    public final Instant shiftStartTime;
    public final Instant shiftEndTime;
    
    // Creation info
    public final Instant createdAt;
    public final String createdBy;
    
    // Current state
    public final String lastUpdatedBy;
    public final String lastActionTaken;
    public final Integer totalHeadCount;
    public final List<String> acceptedAssociateIds;
    public final String fillStatus;
    
    // Optional fields
    public final Instant dropPenaltyStartsAt;
    public final Instant dropEndsAt;
    
    // Change tracking
    public final Boolean headCountChanged;
    public final Boolean acceptedAssociatesChanged;
    
    // Previous state fields (second half as requested)
    public final Instant previousSrcTimestamp;
    public final String previousLastUpdatedBy;
    public final String previousLastActionTaken;
    public final Integer previousTotalHeadCount;
    public final List<String> previousAcceptedAssociateIds;
    
    // Timestamps (ordered: source -> sns -> lambda -> flink)
    public final Instant srcTimestamp;           // Source system timestamp (renamed from eventTime)
    public final Instant snsTimestamp;          // SNS timestamp
    public final Instant lambdaTimestamp;       // Lambda processing timestamp
    public final Instant flinkTimestamp;        // Flink processing timestamp (renamed from processingTimestamp)
    
    // Latency metrics (at the very end)
    public final Long endToEndLatencyMs;         // Flink timestamp - source timestamp (total latency)
    public final Long snsToFlinkLatencyMs;       // Flink timestamp - SNS timestamp (processing latency)

    public ShiftOpportunityLogEvent(
        String eventType,
        String opportunityId,
        String opportunityType,
        String messageId,
        String topicArn,
        String fieldsChanged,
        String entityId,
        String serializationKey,
        Long sequenceNumber,
        String country,
        String locationId,
        String workgroup,
        String source,
        String timezone,
        String batchId,
        Instant claimStartsAt,
        Instant claimEndsAt,
        String claimStatus,
        Instant shiftStartTime,
        Instant shiftEndTime,
        Instant createdAt,
        String createdBy,
        String lastUpdatedBy,
        String lastActionTaken,
        Integer totalHeadCount,
        List<String> acceptedAssociateIds,
        String fillStatus,
        Instant dropPenaltyStartsAt,
        Instant dropEndsAt,
        Boolean headCountChanged,
        Boolean acceptedAssociatesChanged,
        Instant previousSrcTimestamp,
        String previousLastUpdatedBy,
        String previousLastActionTaken,
        Integer previousTotalHeadCount,
        List<String> previousAcceptedAssociateIds,
        Instant srcTimestamp,
        Instant snsTimestamp,
        Instant lambdaTimestamp,
        Instant flinkTimestamp,
        Long endToEndLatencyMs,
        Long snsToFlinkLatencyMs
    ) {
        super(srcTimestamp != null ? srcTimestamp : Instant.now());
        this.eventType = eventType;
        this.opportunityId = opportunityId;
        this.opportunityType = opportunityType;
        this.messageId = messageId;
        this.topicArn = topicArn;
        this.fieldsChanged = fieldsChanged;
        this.entityId = entityId;
        this.serializationKey = serializationKey;
        this.sequenceNumber = sequenceNumber;
        this.country = country;
        this.locationId = locationId;
        this.workgroup = workgroup;
        this.source = source;
        this.timezone = timezone;
        this.batchId = batchId;
        this.claimStartsAt = claimStartsAt;
        this.claimEndsAt = claimEndsAt;
        this.claimStatus = claimStatus;
        this.shiftStartTime = shiftStartTime;
        this.shiftEndTime = shiftEndTime;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.lastUpdatedBy = lastUpdatedBy;
        this.lastActionTaken = lastActionTaken;
        this.totalHeadCount = totalHeadCount;
        this.acceptedAssociateIds = acceptedAssociateIds;
        this.fillStatus = fillStatus;
        this.dropPenaltyStartsAt = dropPenaltyStartsAt;
        this.dropEndsAt = dropEndsAt;
        this.headCountChanged = headCountChanged;
        this.acceptedAssociatesChanged = acceptedAssociatesChanged;
        this.previousSrcTimestamp = previousSrcTimestamp;
        this.previousLastUpdatedBy = previousLastUpdatedBy;
        this.previousLastActionTaken = previousLastActionTaken;
        this.previousTotalHeadCount = previousTotalHeadCount;
        this.previousAcceptedAssociateIds = previousAcceptedAssociateIds;
        this.srcTimestamp = srcTimestamp;
        this.snsTimestamp = snsTimestamp;
        this.lambdaTimestamp = lambdaTimestamp;
        this.flinkTimestamp = flinkTimestamp;
        this.endToEndLatencyMs = endToEndLatencyMs;
        this.snsToFlinkLatencyMs = snsToFlinkLatencyMs;
    }
} 