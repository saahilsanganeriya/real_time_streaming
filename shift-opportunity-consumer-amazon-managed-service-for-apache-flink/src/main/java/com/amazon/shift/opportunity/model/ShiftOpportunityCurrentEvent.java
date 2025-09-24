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
 * Current State Event Model for OpenSearch Indexing
 * =================================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Current state document model for real-time dashboard queries
 * 
 * Description:
 * This class represents the current state of a shift opportunity optimized for
 * real-time dashboard queries and monitoring. It contains the latest state information
 * for each opportunity with computed business fields and performance metrics.
 * 
 * Key Features:
 * - Optimized field ordering for user-friendly JSON output
 * - Immutable design for thread safety in stream processing
 * - Computed business status fields (claim_status, fill_status)
 * - Real-time latency metrics for performance monitoring
 * - Logical field grouping for efficient querying
 * 
 * OpenSearch Index Pattern:
 * - Index: shift-opportunity-current
 * - Pattern: Upsert by opportunity_id
 * - Primary Key: opportunity_id
 * - Update Strategy: Conditional updates based on src_timestamp
 * - Use Case: Real-time dashboards, alerting, current state queries
 * 
 * Business Status Fields:
 * - claim_status: NOT_STARTED (future), ACTIVE (current), EXPIRED (past)
 * - fill_status: OPEN (unfilled), PARTIALLY_FILLED (some accepted), FILLED (complete)
 * 
 * Field Organization:
 * 1. Core Identifiers: Essential business keys for filtering and grouping
 * 2. Location & Organizational: Geographic and departmental context
 * 3. Timing Information: Claim windows and shift schedules
 * 4. Staffing Data: Headcount requirements and assignments
 * 5. Audit Trail: Creation and modification history
 * 6. Performance Metrics: Latency measurements for monitoring
 * 
 * Dependencies:
 * Upstream:
 *   - ShiftOpportunityToCurrentEvent: Transformation operator
 *   - ShiftOpportunityEvent: Source data model from Kinesis
 * 
 * Downstream:
 *   - OpenSearch current state index: Real-time upsert operations
 *   - Dashboard queries: Business intelligence and monitoring
 *   - Alert systems: SLA monitoring and threshold notifications
 * 
 * JSON Output Example:
 * {
 *   "event_type": "OpportunityUpdated",
 *   "opportunity_id": "opp-123",
 *   "opportunity_type": "REGULAR",
 *   "country": "US",
 *   "location_id": "loc-456",
 *   "fill_status": "PARTIALLY_FILLED",
 *   "claim_status": "ACTIVE",
 *   "total_head_count": 5,
 *   "accepted_associate_ids": ["assoc-1", "assoc-2"],
 *   "src_timestamp": "2024-01-01T12:00:00Z",
 *   "flink_timestamp": "2024-01-01T12:00:02Z",
 *   "end_to_end_latency_ms": 2000
 * }
 * 
 * Performance Considerations:
 * - JsonPropertyOrder ensures consistent field ordering for better user experience
 * - Immutable final fields prevent accidental modifications
 * - Efficient serialization with Jackson configuration from Document base class
 * - Optimized for high-frequency upsert operations in OpenSearch
 */

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.time.Instant;
import java.util.List;

/**
 * Immutable data model representing the current state of a shift opportunity.
 * 
 * Optimized for real-time dashboard queries with logical field grouping and
 * comprehensive business status information. Fields are ordered for optimal
 * user experience in JSON output.
 */
@JsonPropertyOrder({
    // Primary identifiers first
    "eventType", "opportunityId", "opportunityType", 
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
    // Timestamps in chronological order
    "srcTimestamp", "snsTimestamp", "lambdaTimestamp", "flinkTimestamp", 
    // Latency metrics at the end
    "endToEndLatencyMs", "snsToFlinkLatencyMs"
})
public class ShiftOpportunityCurrentEvent extends Document {
    // Primary identifiers (ordered for user-friendliness)
    public final String eventType;
    public final String opportunityId;
    public final String opportunityType;
    
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
    
    // Timestamps (ordered: source -> sns -> lambda -> flink)
    public final Instant srcTimestamp;           // Source system timestamp (renamed from eventTime/lastUpdatedAt)
    public final Instant snsTimestamp;          // SNS timestamp
    public final Instant lambdaTimestamp;       // Lambda processing timestamp
    public final Instant flinkTimestamp;        // Flink processing timestamp (renamed from processingTimestamp)
    
    // Latency metrics (at the very end)
    public final Long endToEndLatencyMs;         // Flink timestamp - source timestamp (total latency)
    public final Long snsToFlinkLatencyMs;       // Flink timestamp - SNS timestamp (processing latency)

    public ShiftOpportunityCurrentEvent(
        String eventType,
        String opportunityId,
        String opportunityType,
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
        this.srcTimestamp = srcTimestamp;
        this.snsTimestamp = snsTimestamp;
        this.lambdaTimestamp = lambdaTimestamp;
        this.flinkTimestamp = flinkTimestamp;
        this.endToEndLatencyMs = endToEndLatencyMs;
        this.snsToFlinkLatencyMs = snsToFlinkLatencyMs;
    }
} 