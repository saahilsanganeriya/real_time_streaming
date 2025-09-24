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

package com.amazon.shift.opportunity.serialization;

/**
 * Event Deserialization Schema for Kinesis Stream Processing
 * =========================================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Flink deserialization interface for Kinesis Data Streams
 * 
 * Description:
 * This class provides the deserialization interface between Flink's Kinesis connector
 * and our custom ShiftOpportunityEvent data model. It converts raw bytes from Kinesis
 * stream records into typed Java objects for stream processing.
 * 
 * Key Features:
 * - Graceful error handling for malformed JSON data
 * - Debug logging for troubleshooting parsing issues
 * - Type-safe conversion to ShiftOpportunityEvent objects
 * - Integration with Flink's streaming execution environment
 * 
 * Error Handling Strategy:
 * - Invalid events return null instead of throwing exceptions
 * - Null events are automatically filtered out by Flink
 * - Debug logging captures parsing errors without disrupting the pipeline
 * - Resilient processing continues despite occasional bad data
 * 
 * Dependencies:
 * Upstream:
 *   - Amazon Kinesis Data Streams: Raw byte stream records
 *   - Flink Kinesis connector: Byte array delivery mechanism
 * 
 * Downstream:
 *   - ShiftOpportunityEvent: Target data model
 *   - Flink DataStream operators: Stream processing pipeline
 * 
 * Usage:
 * This schema is configured in the main application when creating the Kinesis source:
 * KinesisStreamsSource.builder().setDeserializationSchema(new EventDeserializationSchema())
 */

import com.amazon.shift.opportunity.model.ShiftOpportunityEvent;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Flink deserialization schema for converting Kinesis bytes to ShiftOpportunityEvent objects.
 * 
 * Implements AbstractDeserializationSchema to integrate with Flink's Kinesis connector.
 * Provides error-resilient parsing with debug logging for troubleshooting.
 */
public class EventDeserializationSchema extends AbstractDeserializationSchema<ShiftOpportunityEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(EventDeserializationSchema.class);

    /**
     * Deserializes raw bytes from Kinesis into a ShiftOpportunityEvent object.
     * 
     * This method is called by Flink for each record received from the Kinesis stream.
     * It delegates the actual parsing to ShiftOpportunityEvent.parseEvent() which
     * handles the complex JSON structure and COALESCE logic.
     * 
     * Error Handling:
     * - Catches all parsing exceptions to prevent pipeline failures
     * - Logs problematic events at DEBUG level for troubleshooting
     * - Returns null for unparseable events (Flink automatically filters these out)
     * - Includes original event data in log messages for debugging
     * 
     * @param bytes Raw byte array from Kinesis stream record
     * @return Parsed ShiftOpportunityEvent or null if parsing fails
     */
    @Override
    public ShiftOpportunityEvent deserialize(byte[] bytes) {
        try {
            return ShiftOpportunityEvent.parseEvent(bytes);
        } catch (Exception e) {
            // Log at DEBUG level to avoid spam, but capture the full event for troubleshooting
            LOG.debug("Failed to parse shift opportunity event: {}", 
                     new String(bytes, StandardCharsets.UTF_8), e);
            
            // Return null to filter out this event (Flink handles null values gracefully)
            return null;
        }
    }

    /**
     * Indicates whether the stream has ended.
     * 
     * For continuous Kinesis streams, this always returns false since we process
     * events indefinitely until the application is stopped.
     * 
     * @param event The deserialized event (unused in this implementation)
     * @return Always false for continuous stream processing
     */
    @Override
    public boolean isEndOfStream(ShiftOpportunityEvent event) {
        return false;
    }

    /**
     * Provides type information for Flink's serialization framework.
     * 
     * Flink uses this information for:
     * - Type-safe serialization/deserialization
     * - Query optimization in the execution engine
     * - Checkpoint/savepoint state management
     * 
     * @return TypeInformation for ShiftOpportunityEvent class
     */
    @Override
    public TypeInformation<ShiftOpportunityEvent> getProducedType() {
        return TypeExtractor.getForClass(ShiftOpportunityEvent.class);
    }

}
