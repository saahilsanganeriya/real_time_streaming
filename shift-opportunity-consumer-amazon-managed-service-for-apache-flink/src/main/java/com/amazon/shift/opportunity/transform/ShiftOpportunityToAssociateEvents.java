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
 * Associate Event Expansion Transformation Operator
 * =================================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Transform one-to-many relationships for associate-level analytics
 * 
 * Description:
 * This operator performs a critical normalization transformation that expands each
 * ShiftOpportunityEvent into multiple ShiftOpportunityAssociateEvent records - one for
 * each accepted associate. This enables detailed associate-level analytics, tracking
 * individual assignments, and supporting complex queries across associate data.
 * 
 * Transformation Logic:
 * - FlatMap operation: 1 opportunity event → N associate events
 * - One output record per accepted associate ID
 * - Handles associate additions, removals, and complete opportunity deletions
 * - Generates composite document IDs for efficient OpenSearch operations
 * - Supports deletion operations for proper index maintenance
 * 
 * Key Features:
 * - One-to-many relationship expansion for normalized analytics
 * - Deletion support for removed associates with proper cleanup
 * - Composite document ID generation (opportunity_id + "#" + associate_id)
 * - Complete opportunity deletion handling
 * - Empty list handling with no output generation
 * - Performance metrics calculation for each associate record
 * 
 * Associate Lifecycle Management:
 * - New Assignments: Creates new associate records when associates are added
 * - Removed Assignments: Generates deletion records when associates are removed
 * - Complete Deletion: Handles opportunity deletion by removing all associate records
 * - Change Tracking: Supports complex assignment change scenarios
 * 
 * Event Type Handling:
 * - OpportunityCreated: Creates records for initially assigned associates
 * - OpportunityUpdated: Handles associate additions and removals
 * - OpportunityDeleted: Removes all existing associate records
 * 
 * Deletion Strategy:
 * - Compare current vs previous associate lists to detect removals
 * - Generate deletion records (isDeleted=true) for removed associates
 * - Complete opportunity deletion removes all associated records
 * - Maintains index cleanliness through proper lifecycle management
 * 
 * Target Use Cases:
 * - Associate performance analytics and KPI tracking
 * - Workload distribution analysis across associates
 * - Individual assignment frequency and pattern analysis
 * - Associate utilization metrics and capacity planning
 * - Geographic assignment distribution studies
 * - Peak time associate workload analysis
 * 
 * Dependencies:
 * Upstream:
 *   - ShiftOpportunityEvent: Input data model with acceptedAssociateIds array
 *   - Main processing pipeline: Event filtering and validation
 * 
 * Downstream:
 *   - ShiftOpportunityAssociateEvent: Normalized output data model
 *   - Associates OpenSearch index: Upsert and delete operations
 *   - Associate analytics queries: Performance and utilization analysis
 * 
 * Performance Considerations:
 * - FlatMap enables efficient parallel processing of associate records
 * - Composite document IDs prevent conflicts in concurrent processing
 * - Optimized for analytical queries across large associate datasets
 * - Efficient deletion handling maintains index performance
 * - Stateless design supports horizontal scaling
 * 
 * Analytics Capabilities:
 * - Individual associate assignment tracking
 * - Cross-opportunity associate performance analysis
 * - Time-series analysis of associate workloads
 * - Geographic and temporal assignment patterns
 * - Associate capacity and utilization metrics
 * - Fill rate analysis per associate
 */

import com.amazon.shift.opportunity.model.ShiftOpportunityAssociateEvent;
import com.amazon.shift.opportunity.model.ShiftOpportunityEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;

/**
 * Transformation operator for expanding opportunities into individual associate records.
 * 
 * Implements FlatMapFunction for one-to-many transformation, creating separate documents
 * for each associate assignment to enable normalized analytics and proper deletion handling.
 */
public class ShiftOpportunityToAssociateEvents implements FlatMapFunction<ShiftOpportunityEvent, ShiftOpportunityAssociateEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ShiftOpportunityToAssociateEvents.class);

    /**
     * Expands a single ShiftOpportunityEvent into multiple ShiftOpportunityAssociateEvent records.
     * 
     * This is the core normalization logic that transforms the one-to-many relationship
     * between opportunities and associates into individual records for each associate.
     * The method handles complex scenarios including associate additions, removals,
     * and complete opportunity deletions.
     * 
     * Transformation Logic:
     * 1. Calculate performance metrics for each associate record
     * 2. Handle special case of opportunity deletion (remove all associate records)
     * 3. Compare current vs previous associate lists to detect changes
     * 4. Generate deletion records for removed associates
     * 5. Generate update/create records for current associates
     * 
     * Event Processing Scenarios:
     * 
     * OpportunityCreated:
     * - Previous associates list is empty
     * - Current associates list contains initial assignments
     * - Output: Create records for all current associates
     * 
     * OpportunityUpdated:
     * - Previous associates list contains old assignments
     * - Current associates list contains new assignments
     * - Output: Delete records for removed associates + Update records for current associates
     * 
     * OpportunityDeleted:
     * - Use available associate list (current or previous)
     * - Output: Delete records for all previously assigned associates
     * 
     * Associate Change Detection:
     * - Uses Set operations to efficiently detect additions and removals
     * - Handles null values and empty strings gracefully
     * - Trims whitespace for data consistency
     * 
     * Deletion Strategy:
     * - Generates deletion records (isDeleted=true) for removed associates
     * - Maintains referential integrity in the associates index
     * - Supports both individual removals and complete opportunity deletion
     * 
     * Performance Optimization:
     * - Uses HashSet for efficient set operations
     * - Minimal object creation in processing loop
     * - Debug logging for troubleshooting without performance impact
     * 
     * @param event Input opportunity event from Kinesis stream
     * @param out Collector for emitting individual associate events
     * @throws Exception If transformation fails (propagated to Flink error handling)
     */
    @Override
    public void flatMap(ShiftOpportunityEvent event, Collector<ShiftOpportunityAssociateEvent> out) throws Exception {
        
        // ===== PERFORMANCE METRICS CALCULATION =====
        Instant flinkTimestamp = Instant.now();
        
        Long endToEndLatencyMs = null;
        Long snsToFlinkLatencyMs = null;
        
        if (event.srcTimestamp != null) {
            endToEndLatencyMs = flinkTimestamp.toEpochMilli() - event.srcTimestamp.toEpochMilli();
        }
        
        if (event.snsTimestamp != null) {
            snsToFlinkLatencyMs = flinkTimestamp.toEpochMilli() - event.snsTimestamp.toEpochMilli();
        }

        // ===== OPPORTUNITY DELETION HANDLING =====
        if ("OpportunityDeleted".equals(event.eventType)) {
            /*
             * Complete opportunity deletion requires removing all associate records.
             * We use whichever associate list is available (current or previous)
             * since the source system may handle deletions differently.
             */
            List<String> associatesToDelete = event.acceptedAssociateIds != null ? 
                event.acceptedAssociateIds : event.previousAcceptedAssociateIds;
                
            if (associatesToDelete != null && !associatesToDelete.isEmpty()) {
                for (String associateId : associatesToDelete) {
                    if (associateId != null && !associateId.trim().isEmpty()) {
                        emitAssociateEvent(event, associateId.trim(), true, flinkTimestamp, 
                                         endToEndLatencyMs, snsToFlinkLatencyMs, out);
                    }
                }
                LOG.debug("Emitted deletion records for {} associates of deleted opportunity {}", 
                         associatesToDelete.size(), event.opportunityId);
            }
            return; // Early exit for deletion events
        }

        // ===== ASSOCIATE LIST NORMALIZATION =====
        Set<String> currentAssociates = new HashSet<>();
        Set<String> previousAssociates = new HashSet<>();
        
        // Process current associate assignments
        if (event.acceptedAssociateIds != null) {
            for (String id : event.acceptedAssociateIds) {
                if (id != null && !id.trim().isEmpty()) {
                    currentAssociates.add(id.trim()); // Normalize whitespace
                }
            }
        }
        
        // Process previous associate assignments for change detection
        if (event.previousAcceptedAssociateIds != null) {
            for (String id : event.previousAcceptedAssociateIds) {
                if (id != null && !id.trim().isEmpty()) {
                    previousAssociates.add(id.trim()); // Normalize whitespace
                }
            }
        }

        // ===== CHANGE DETECTION AND DELETION HANDLING =====
        /*
         * Detect removed associates using set difference operation.
         * Associates in previous list but not in current list have been removed
         * and require deletion records to maintain index consistency.
         */
        Set<String> removedAssociates = new HashSet<>(previousAssociates);
        removedAssociates.removeAll(currentAssociates);
        
        // Generate deletion records for removed associates
        for (String associateId : removedAssociates) {
            emitAssociateEvent(event, associateId, true, flinkTimestamp, 
                             endToEndLatencyMs, snsToFlinkLatencyMs, out);
            LOG.debug("Emitted deletion record for removed associate {} from opportunity {}", 
                     associateId, event.opportunityId);
        }

        // ===== CURRENT ASSOCIATE RECORD GENERATION =====
        /*
         * Generate update/create records for all currently assigned associates.
         * This handles both new assignments and updates to existing assignments.
         */
        for (String associateId : currentAssociates) {
            emitAssociateEvent(event, associateId, false, flinkTimestamp, 
                             endToEndLatencyMs, snsToFlinkLatencyMs, out);
            LOG.debug("Emitted update record for current associate {} in opportunity {}", 
                     associateId, event.opportunityId);
        }
        
        LOG.debug("Processed opportunity {}: {} current associates, {} removed associates", 
                 event.opportunityId, currentAssociates.size(), removedAssociates.size());
    }

    /**
     * Creates and emits a single associate event record.
     * 
     * This helper method constructs individual ShiftOpportunityAssociateEvent objects
     * from the parent opportunity event data combined with associate-specific information.
     * It handles both creation/update operations and deletion operations.
     * 
     * Field Mapping Strategy:
     * - Direct mapping for most opportunity fields
     * - Associate-specific fields: associateId, documentId generation
     * - Type conversion: totalHeadCount (Integer → Double) for analytics
     * - Deletion flag: isDeleted controls record lifecycle
     * - Performance metrics: latency calculations for monitoring
     * 
     * Document ID Generation:
     * - Composite key: opportunity_id + "#" + associate_id
     * - Enables efficient OpenSearch operations
     * - Prevents conflicts in concurrent processing
     * - Supports both upsert and delete operations
     * 
     * Data Type Considerations:
     * - totalHeadCount converted to Double for analytical operations
     * - All timestamp fields preserved for audit trails
     * - Null-safe handling for optional fields
     * 
     * @param event Parent opportunity event containing shared data
     * @param associateId Individual associate identifier
     * @param isDeleted Whether this record represents a deletion operation
     * @param flinkTimestamp Processing completion timestamp
     * @param endToEndLatencyMs Total latency from source to processing
     * @param snsToFlinkLatencyMs Processing pipeline latency
     * @param out Collector for emitting the created associate event
     */
    private void emitAssociateEvent(ShiftOpportunityEvent event, String associateId, boolean isDeleted,
                                   Instant flinkTimestamp, Long endToEndLatencyMs, Long snsToFlinkLatencyMs,
                                   Collector<ShiftOpportunityAssociateEvent> out) {
        
        // Create individual associate record with normalized data
        ShiftOpportunityAssociateEvent associateEvent = new ShiftOpportunityAssociateEvent(
            // === CORE IDENTIFIERS ===
            event.eventType,           // Event type from parent opportunity
            event.opportunityId,       // Parent opportunity identifier
            associateId,               // Individual associate identifier
            event.opportunityType,     // Business category
            isDeleted,                 // Lifecycle flag (create/update vs delete)
            
            // === LOCATION & ORGANIZATIONAL DATA ===
            event.country,             // Geographic classification
            event.locationId,          // Physical location identifier
            event.workgroup,           // Organizational unit
            event.source,              // Source system identifier
            event.timezone,            // Timezone for shift scheduling
            event.batchId,             // Batch processing identifier
            
            // === TIMING INFORMATION ===
            event.claimStartsAt,       // When associates can claim
            event.claimEndsAt,         // When claiming period ends
            event.claimStatus,         // Current claim status
            event.shiftStartTime,      // Actual work start time
            event.shiftEndTime,        // Actual work end time
            
            // === CREATION & MODIFICATION TRACKING ===
            event.createdAt,           // When opportunity was created
            event.createdBy,           // Who created the opportunity
            event.lastUpdatedBy,       // Most recent modifier
            event.lastActionTaken,     // Last action performed
            
            // === CAPACITY AND ANALYTICS DATA ===
            event.totalHeadCount != null ? event.totalHeadCount.doubleValue() : null, // Convert to Double for analytics
            event.fillStatus,          // Current fill status
            
            // === OPTIONAL BUSINESS FIELDS ===
            event.dropPenaltyStartsAt, // Drop penalty timing (if applicable)
            event.dropEndsAt,          // Drop penalty end (if applicable)
            
            // === TIMESTAMP CHAIN FOR AUDIT ===
            event.srcTimestamp,        // Source system timestamp
            event.snsTimestamp,        // SNS publication timestamp
            event.lambdaTimestamp,     // Lambda processing timestamp
            flinkTimestamp,            // Flink processing completion timestamp
            
            // === PERFORMANCE METRICS ===
            endToEndLatencyMs,         // Total system latency
            snsToFlinkLatencyMs        // Real-time processing latency
        );

        // Emit the associate event to the stream
        out.collect(associateEvent);
    }
} 