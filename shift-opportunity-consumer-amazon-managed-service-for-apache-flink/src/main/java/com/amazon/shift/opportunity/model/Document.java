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
 * Base Document Class for OpenSearch Event Models
 * ===============================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Abstract base class for all OpenSearch document types
 * 
 * Description:
 * This abstract class provides common functionality for all OpenSearch document models
 * in the shift opportunity processing pipeline. It standardizes JSON serialization,
 * timestamp handling, and ensures consistent field naming across all document types.
 * 
 * Key Features:
 * - Standardized Jackson configuration for consistent JSON output
 * - Snake case field naming to match OpenSearch/Elasticsearch conventions
 * - Proper Java 8 time handling with ISO-8601 format
 * - Thread-safe singleton ObjectMapper for performance
 * - Immutable timestamp field for event ordering
 * 
 * JSON Serialization Strategy:
 * - Snake case field names (e.g., opportunityId -> opportunity_id)
 * - ISO-8601 timestamp format for cross-system compatibility
 * - Excludes internal timestamp field from JSON output
 * - Handles null values gracefully
 * 
 * Dependencies:
 * Upstream:
 *   - Transformation operators: Create document instances
 *   - ShiftOpportunityEvent: Source of timestamp information
 * 
 * Downstream:
 *   - OpenSearch sinks: JSON serialization for indexing
 *   - S3 backup: JSON string representation for file storage
 * 
 * Subclasses:
 *   - ShiftOpportunityCurrentEvent: Current state documents
 *   - ShiftOpportunityLogEvent: Audit trail documents
 *   - ShiftOpportunityAssociateEvent: Associate-level documents
 * 
 * Performance Considerations:
 * - Static ObjectMapper reduces object creation overhead
 * - Configured once at class loading for thread safety
 * - Efficient JSON serialization for high-throughput processing
 * - Immutable design prevents accidental field modifications
 */

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Abstract base class for all OpenSearch document types.
 * 
 * Provides standardized JSON serialization and timestamp handling for consistent
 * document formatting across all OpenSearch indexes in the pipeline.
 */
public abstract class Document {
  
  /**
   * Shared ObjectMapper instance for consistent JSON serialization across all document types.
   * 
   * Configuration:
   * - Snake case property naming: Java camelCase fields become snake_case in JSON
   * - Java 8 time module: Proper handling of Instant, LocalDateTime, etc.
   * - ISO-8601 timestamp format: Standard format for cross-system compatibility
   * - Thread-safe: Can be used concurrently by multiple Flink operators
   */
  private static final ObjectMapper mapper = new ObjectMapper()
      .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
      .registerModule(new JavaTimeModule())
      .configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

  /**
   * Internal timestamp field for document creation tracking.
   * 
   * This field is excluded from JSON serialization (@JsonIgnore) because:
   * - The actual timestamp data is included in subclass fields (src_timestamp, flink_timestamp, etc.)
   * - Avoiding duplicate timestamp fields in the final JSON output
   * - Used internally for ordering and debugging purposes
   */
  @JsonIgnore
  public final String timestamp;

  /**
   * Constructs a Document with the specified timestamp.
   * 
   * The timestamp is converted to ISO-8601 string format for consistent handling
   * across the system. If no timestamp is provided, uses the current system time.
   * 
   * @param timestamp The timestamp for this document, or null to use current time
   */
  public Document(Instant timestamp) {
    this.timestamp = timestamp != null ? 
        DateTimeFormatter.ISO_INSTANT.format(timestamp) : 
        DateTimeFormatter.ISO_INSTANT.format(Instant.now());
  }

  /**
   * Serializes this document to JSON string format.
   * 
   * This method is used by:
   * - OpenSearch sinks for document indexing
   * - S3 backup for file storage
   * - Logging and debugging output
   * 
   * The JSON output follows snake_case naming conventions and includes all
   * non-@JsonIgnore fields from this class and its subclasses.
   * 
   * @return JSON string representation of this document
   * @throws RuntimeException if JSON serialization fails
   */
  @Override
  public String toString() {
    try {
      return mapper.writeValueAsString(this);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize document to JSON", e);
    }
  }
}
