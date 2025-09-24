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

package com.amazon.shift.opportunity.transform;

/**
 * Audit Log Event Transformation Operator
 * =======================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Transform input events to audit log format for compliance and debugging
 * 
 * Description:
 * This operator transforms ShiftOpportunityEvent objects into ShiftOpportunityLogEvent
 * objects optimized for audit trail, compliance, and debugging purposes. The transformation
 * preserves comprehensive event information including metadata, change tracking, and
 * previous state information.
 * 
 * Transformation Logic:
 * - Preserves all event metadata from SNS transport layer
 * - Maps complete business data with change detection flags
 * - Includes previous state information for comparison
 * - Computes real-time latency metrics for performance monitoring
 * - Maintains complete audit trail for compliance requirements
 * 
 * Key Features:
 * - Complete event preservation for regulatory compliance
 * - Change tracking with boolean flags for modified fields
 * - SNS metadata retention for debugging and correlation
 * - Previous state preservation for detailed change analysis
 * - Field-level change detection for granular audit trails
 * 
 * Target Use Cases:
 * - Regulatory compliance and audit trails
 * - Debugging and troubleshooting event processing issues
 * - Historical analytics and trend analysis
 * - Change impact analysis and event sourcing
 * - Performance monitoring and latency tracking
 * 
 * Compliance Features:
 * - Immutable audit trail preservation
 * - Complete event metadata retention
 * - Change tracking for regulatory requirements
 * - Long-term retention through S3 backup integration
 * - Event sourcing capabilities for system reconstruction
 * 
 * Dependencies:
 * Upstream:
 *   - ShiftOpportunityEvent: Input data model from Kinesis
 *   - Main processing pipeline: Event filtering and validation
 * 
 * Downstream:
 *   - ShiftOpportunityLogEvent: Output data model for OpenSearch
 *   - Events log OpenSearch index: Append-only audit trail
 *   - S3 backup: Long-term retention for compliance
 * 
 * Performance Considerations:
 * - Stateless transformation for horizontal scaling
 * - Comprehensive field mapping for complete audit trails
 * - Efficient serialization for high-volume append operations
 * - Optimized for write-heavy, read-light access patterns
 * - Real-time latency calculation for immediate visibility
 * 
 * Debugging Support:
 * - SNS message ID preservation for correlation
 * - Topic ARN retention for source identification
 * - Sequence number preservation for event ordering
 * - Complete timestamp chain for timing analysis
 * - Field change tracking for root cause analysis
 */

import com.amazon.shift.opportunity.model.ShiftOpportunityLogEvent;
import com.amazon.shift.opportunity.model.ShiftOpportunityEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.Duration;

/**
 * Transformation operator for converting input events to audit log format.
 * 
 * Implements MapFunction for one-to-one event transformation, optimized for
 * comprehensive audit trails and compliance requirements.
 */
public class ShiftOpportunityToLogEvent implements MapFunction<ShiftOpportunityEvent, ShiftOpportunityLogEvent> {

    /**
     * Transforms a ShiftOpportunityEvent into a ShiftOpportunityLogEvent.
     * 
     * This transformation creates a comprehensive audit log entry that preserves
     * all event information for compliance, debugging, and analytical purposes.
     * 
     * Key Transformations:
     * - Complete field mapping with no data loss
     * - SNS metadata preservation for debugging and correlation
     * - Change tracking flags for granular audit analysis
     * - Previous state information for comparison and impact analysis
     * - Real-time latency calculation for performance monitoring
     * 
     * Audit Trail Features:
     * - Immutable record of all event data
     * - Complete timestamp chain for timing analysis
     * - Field-level change detection for compliance
     * - Previous state preservation for impact assessment
     * - SNS correlation data for end-to-end debugging
     * 
     * Performance Metrics:
     * - End-to-end latency: Source system timestamp to Flink processing completion
     * - SNS-to-Flink latency: Real-time processing pipeline performance
     * - These metrics enable SLA monitoring and performance optimization
     * 
     * @param event Input event from Kinesis stream
     * @return Audit log event optimized for OpenSearch indexing and long-term retention
     * @throws Exception If transformation fails (propagated to Flink error handling)
     */
    @Override
    public ShiftOpportunityLogEvent map(ShiftOpportunityEvent event) throws Exception {
        // Capture processing timestamp for latency calculations
        Instant flinkTimestamp = Instant.now();
        
        // Ensure we have a valid source timestamp for audit trail
        Instant srcTimestamp = event.srcTimestamp != null ? event.srcTimestamp : Instant.now();
        
        // Calculate performance metrics for monitoring and SLA tracking
        long endToEndLatencyMs = Duration.between(srcTimestamp, flinkTimestamp).toMillis();
        long snsToFlinkLatencyMs = event.snsTimestamp != null ? 
            Duration.between(event.snsTimestamp, flinkTimestamp).toMillis() : 0L;

        // Build comprehensive audit log event with complete information preservation
        return new ShiftOpportunityLogEvent(
            // === CORE IDENTIFIERS ===
            event.eventType,           // Event classification for audit filtering
            event.opportunityId,       // Primary business key for correlation
            event.opportunityType,     // Business category for analysis
            
            // === SNS TRANSPORT METADATA ===
            event.messageId,           // SNS message ID for deduplication and correlation
            event.topicArn,            // Source topic identification for debugging
            event.fieldsChanged,       // Field-level change tracking for compliance
            event.entityId,            // Business entity correlation
            event.serializationKey,    // Source system serialization information
            event.sequenceNumber,      // Event ordering information
            
            // === LOCATION & ORGANIZATIONAL DATA ===
            event.country,             // Geographic classification
            event.locationId,          // Physical location identifier
            event.workgroup,           // Organizational unit
            event.source,              // Source system identifier
            event.timezone,            // Timezone for shift scheduling
            event.batchId,             // Batch processing identifier
            
            // === CLAIM TIMING ===
            event.claimStartsAt,       // When associates can claim
            event.claimEndsAt,         // When claiming period ends
            event.claimStatus,         // Computed claim status
            
            // === SHIFT TIMING ===
            event.shiftStartTime,      // Actual work start time
            event.shiftEndTime,        // Actual work end time
            
            // === CREATION & MODIFICATION TRACKING ===
            event.createdAt,           // Initial creation timestamp
            event.createdBy,           // Original creator
            event.lastUpdatedBy,       // Most recent modifier
            event.lastActionTaken,     // Last action performed
            
            // === STAFFING INFORMATION ===
            event.totalHeadCount,      // Required number of associates
            event.acceptedAssociateIds, // List of accepted associates
            event.fillStatus,          // Computed fill status
            
            // === OPTIONAL BUSINESS FIELDS ===
            event.dropPenaltyStartsAt, // Drop penalty timing (if applicable)
            event.dropEndsAt,          // Drop penalty end (if applicable)
            
            // === CHANGE DETECTION FLAGS ===
            event.headCountChanged,    // Boolean flag for headcount modifications
            event.acceptedAssociatesChanged, // Boolean flag for associate list changes
            
            // === PREVIOUS STATE PRESERVATION ===
            event.previousSrcTimestamp,    // Previous event timestamp for comparison
            event.previousLastUpdatedBy,  // Previous modifier for change tracking
            event.previousLastActionTaken, // Previous action for impact analysis
            event.previousTotalHeadCount, // Previous headcount for delta calculation
            event.previousAcceptedAssociateIds, // Previous associates for change analysis
            
            // === TIMESTAMP CHAIN FOR AUDIT ===
            srcTimestamp,              // Source system timestamp (authoritative)
            event.snsTimestamp,        // SNS publication timestamp
            event.lambdaTimestamp,     // Lambda processing timestamp
            flinkTimestamp,            // Flink processing completion timestamp
            
            // === PERFORMANCE METRICS ===
            endToEndLatencyMs,         // Total system latency for SLA monitoring
            snsToFlinkLatencyMs        // Real-time processing latency
        );
    }
} 