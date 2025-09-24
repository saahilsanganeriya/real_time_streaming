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
 * Associate-Level Event Model for OpenSearch Indexing
 * ===================================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Normalized associate-level document model for detailed analytics
 * 
 * Description:
 * This class represents a normalized view where each associate assignment gets its own
 * document in OpenSearch. This enables detailed associate-level analytics, tracking
 * individual assignments, and supporting both additions and removals of associates
 * from opportunities with proper deletion handling.
 * 
 * Key Features:
 * - One document per (opportunity_id, associate_id) combination
 * - Normalized data model for associate-level analytics
 * - Deletion support for removed associate assignments
 * - Composite document ID for efficient operations
 * - Immutable design for data consistency
 * - Performance metrics for individual assignments
 * 
 * OpenSearch Index Pattern:
 * - Index: shift-opportunity-associates
 * - Pattern: Upsert with deletion support
 * - Primary Key: document_id (opportunity_id + "#" + associate_id)
 * - Update Strategy: Conditional updates with deletion handling
 * - Use Case: Associate analytics, assignment tracking, workload analysis
 * 
 * Normalization Strategy:
 * - Flattens one-to-many relationship (opportunity → associates)
 * - Each accepted associate gets individual document
 * - Enables efficient associate-level queries and aggregations
 * - Supports complex analytics across associate assignments
 * 
 * Deletion Handling:
 * - is_deleted flag indicates removal from opportunity
 * - Proper cleanup when associates are removed
 * - Maintains data consistency during assignment changes
 * - Supports both individual removals and complete opportunity deletion
 * 
 * Document ID Strategy:
 * - Format: "{opportunity_id}#{associate_id}"
 * - Example: "opp-123#assoc-456"
 * - Enables efficient lookups and updates
 * - Prevents conflicts in concurrent processing
 * 
 * Field Organization:
 * 1. Identity Fields: Composite ID and individual identifiers
 * 2. Operation Type: Deletion flag for lifecycle management
 * 3. Business Context: Opportunity and organizational information
 * 4. Assignment Data: Associate-specific metrics and counts
 * 5. Audit Trail: Timestamp chain for tracking
 * 6. Performance Metrics: Latency measurements
 * 
 * Dependencies:
 * Upstream:
 *   - ShiftOpportunityToAssociateEvents: Transformation operator (flatMap)
 *   - ShiftOpportunityEvent: Source data with accepted_associate_ids array
 * 
 * Downstream:
 *   - OpenSearch associates index: Upsert and delete operations
 *   - Associate analytics: Individual performance tracking
 *   - Workload analysis: Assignment distribution queries
 *   - Capacity planning: Associate utilization metrics
 * 
 * Analytics Use Cases:
 * - Associate assignment frequency analysis
 * - Workload distribution across associates
 * - Opportunity fill rate per associate
 * - Assignment timeline and duration tracking
 * - Geographic assignment patterns
 * - Peak time associate utilization
 * 
 * JSON Output Example:
 * {
 *   "document_id": "opp-123#assoc-456",
 *   "opportunity_id": "opp-123",
 *   "associate_id": "assoc-456",
 *   "event_type": "OpportunityUpdated",
 *   "is_deleted": false,
 *   "total_head_count": 5.0,
 *   "accepted_associate_count": 3,
 *   "location_id": "loc-789",
 *   "country": "US",
 *   "src_timestamp": "2024-01-01T12:00:00Z"
 * }
 * 
 * Performance Considerations:
 * - Composite document ID enables efficient upserts
 * - Optimized for analytical queries across associates
 * - Supports both individual and aggregate analytics
 * - Efficient deletion handling maintains index cleanliness
 * - JsonPropertyOrder optimizes field access patterns
 */

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.time.Instant;

@JsonPropertyOrder({
    // Primary identifiers first  
    "documentId", "eventType", "opportunityId", "associateId", "opportunityType",
    // Operation type for deletion handling
    "isDeleted",
    // Location and metadata
    "country", "locationId", "workgroup", "source", "timezone", "batchId",
    // Claim fields grouped
    "claimStartsAt", "claimEndsAt", "claimStatus",
    // Shift timing
    "shiftStartTime", "shiftEndTime", 
    // Creation info
    "createdAt", "createdBy",
    // Current state
    "lastUpdatedBy", "lastActionTaken", "totalHeadCount", "fillStatus",
    // Optional fields
    "dropPenaltyStartsAt", "dropEndsAt",
    // Timestamps in chronological order
    "srcTimestamp", "snsTimestamp", "lambdaTimestamp", "flinkTimestamp",
    // Latency metrics
    "endToEndLatencyMs", "snsToFlinkLatencyMs"
})
public class ShiftOpportunityAssociateEvent extends Document {

    // Composite document ID for OpenSearch
    public final String documentId;
    
    // Primary identifiers
    public final String eventType;
    public final String opportunityId;
    public final String associateId;  // Individual associate ID from the array
    public final String opportunityType;
    
    // Operation type - true if this record represents a deletion
    public final Boolean isDeleted;
    
    // Location and metadata
    public final String country;
    public final String locationId;
    public final String workgroup;
    public final String source;
    public final String timezone;
    public final String batchId;
    
    // Claim fields (grouped)
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
    public final Double totalHeadCount;
    public final String fillStatus; // Note: not including acceptedAssociateIds array since this is for individual associates
    
    // Optional fields
    public final Instant dropPenaltyStartsAt;
    public final Instant dropEndsAt;
    
    // Timestamps in chronological order
    public final Instant srcTimestamp;
    public final Instant snsTimestamp;
    public final Instant lambdaTimestamp;
    public final Instant flinkTimestamp;
    
    // Latency metrics
    public final Long endToEndLatencyMs;
    public final Long snsToFlinkLatencyMs;

    // Composite primary key for OpenSearch document ID
    @JsonIgnore
    public String getDocumentId() {
        return opportunityId + "_" + associateId;
    }

    public ShiftOpportunityAssociateEvent(
            String eventType,
            String opportunityId,
            String associateId,
            String opportunityType,
            Boolean isDeleted,
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
            Double totalHeadCount,
            String fillStatus,
            Instant dropPenaltyStartsAt,
            Instant dropEndsAt,
            Instant srcTimestamp,
            Instant snsTimestamp,
            Instant lambdaTimestamp,
            Instant flinkTimestamp,
            Long endToEndLatencyMs,
            Long snsToFlinkLatencyMs) {

        super(srcTimestamp);  // Pass srcTimestamp to parent Document class for versioning
        
        this.documentId = opportunityId + "_" + associateId;
        this.eventType = eventType;
        this.opportunityId = opportunityId;
        this.associateId = associateId;
        this.opportunityType = opportunityType;
        this.isDeleted = isDeleted;
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