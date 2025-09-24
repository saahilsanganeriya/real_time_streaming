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
 * Current State Event Transformation Operator
 * ===========================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Transform input events to current state format for real-time dashboards
 * 
 * Description:
 * This operator transforms ShiftOpportunityEvent objects into ShiftOpportunityCurrentEvent
 * objects optimized for real-time dashboard queries. The transformation focuses on
 * providing the latest state of each opportunity with computed business fields
 * and performance metrics.
 * 
 * Transformation Logic:
 * - Maps all relevant business fields from input to output format
 * - Computes real-time latency metrics (end-to-end and SNS-to-Flink)
 * - Preserves timestamp chain for audit and monitoring purposes
 * - Organizes fields logically for efficient querying and indexing
 * 
 * Target Use Cases:
 * - Real-time dashboards showing current opportunity status
 * - Alert systems monitoring opportunity fill rates
 * - Performance monitoring with latency metrics
 * - Business intelligence queries on current state
 * 
 * Dependencies:
 * Upstream:
 *   - ShiftOpportunityEvent: Input data model from Kinesis
 *   - Main processing pipeline: Event filtering and validation
 * 
 * Downstream:
 *   - ShiftOpportunityCurrentEvent: Output data model for OpenSearch
 *   - Current state OpenSearch index: Real-time upsert operations
 * 
 * Performance Considerations:
 * - Stateless transformation for horizontal scaling
 * - Minimal computation to maintain low latency
 * - Efficient field mapping without data copying where possible
 * - Real-time timestamp calculation for immediate latency visibility
 */

import com.amazon.shift.opportunity.model.ShiftOpportunityCurrentEvent;
import com.amazon.shift.opportunity.model.ShiftOpportunityEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.Duration;

/**
 * Transformation operator for converting input events to current state format.
 * 
 * Implements MapFunction for one-to-one event transformation, optimized for
 * real-time dashboard queries and monitoring use cases.
 */
public class ShiftOpportunityToCurrentEvent implements MapFunction<ShiftOpportunityEvent, ShiftOpportunityCurrentEvent> {

    /**
     * Transforms a ShiftOpportunityEvent into a ShiftOpportunityCurrentEvent.
     * 
     * This transformation optimizes the data structure for current state queries while
     * preserving all essential business information and adding performance metrics.
     * 
     * Key Transformations:
     * - Direct field mapping for business data (no data loss)
     * - Real-time latency calculation for monitoring
     * - Timestamp preservation for audit trails
     * - Logical field grouping for efficient indexing
     * 
     * Latency Metrics:
     * - End-to-end latency: Source system timestamp to Flink processing completion
     * - SNS-to-Flink latency: SNS publication to Flink processing completion
     * - These metrics enable real-time performance monitoring and SLA tracking
     * 
     * @param event Input event from Kinesis stream
     * @return Current state event optimized for OpenSearch indexing
     * @throws Exception If transformation fails (propagated to Flink error handling)
     */
    @Override
    public ShiftOpportunityCurrentEvent map(ShiftOpportunityEvent event) throws Exception {
        // Capture processing timestamp for latency calculations
        Instant flinkTimestamp = Instant.now();
        
        // Ensure we have a valid source timestamp for latency calculations
        Instant srcTimestamp = event.srcTimestamp != null ? event.srcTimestamp : Instant.now();
        
        // Calculate performance metrics for monitoring and alerting
        long endToEndLatencyMs = Duration.between(srcTimestamp, flinkTimestamp).toMillis();
        long snsToFlinkLatencyMs = event.snsTimestamp != null ? 
            Duration.between(event.snsTimestamp, flinkTimestamp).toMillis() : 0L;

        // Build current state event with logical field grouping
        return new ShiftOpportunityCurrentEvent(
            // === CORE IDENTIFIERS ===
            event.eventType,           // Event classification for filtering
            event.opportunityId,       // Primary business key
            event.opportunityType,     // Business category
            
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
            event.claimStatus,         // Computed claim status (NOT_STARTED, ACTIVE, EXPIRED)
            
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
            event.fillStatus,          // Computed fill status (OPEN, PARTIALLY_FILLED, FILLED)
            
            // === OPTIONAL BUSINESS FIELDS ===
            event.dropPenaltyStartsAt, // Drop penalty timing (if applicable)
            event.dropEndsAt,          // Drop penalty end (if applicable)
            
            // === TIMESTAMP CHAIN FOR AUDIT ===
            srcTimestamp,              // Source system timestamp (authoritative)
            event.snsTimestamp,        // SNS publication timestamp
            event.lambdaTimestamp,     // Lambda processing timestamp
            flinkTimestamp,            // Flink processing completion timestamp
            
            // === PERFORMANCE METRICS ===
            endToEndLatencyMs,         // Total system latency
            snsToFlinkLatencyMs        // Real-time processing latency
        );
    }
} 