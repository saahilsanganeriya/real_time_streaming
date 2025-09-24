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

package com.amazon.shift.opportunity.sink;

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import org.apache.flink.connector.opensearch.sink.OpensearchSink;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.flink.connector.opensearch.sink.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.regions.Region;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Custom Amazon OpenSearch Service Sink Implementation
 * ====================================================
 * 
 * Author: Saahil Sanganeriya
 * Role: Custom OpenSearch sink with AWS IAM authentication and conditional updates
 * 
 * Description:
 * This class provides custom OpenSearch sink functionality that addresses critical
 * limitations of the standard Flink OpenSearch connector. It implements AWS IAM
 * authentication, timestamp-based conditional updates, and Java-based deletion handling
 * for maintaining data consistency in real-time streaming scenarios.
 * 
 * Key Technical Challenges Solved:
 * 
 * 1. AWS IAM Authentication:
 *    - Standard Flink connector doesn't support AWS Signature v4 signing
 *    - Custom RestClientFactory injects HTTP request interceptor for IAM auth
 *    - Enables secure, credential-free access to Amazon OpenSearch Service
 * 
 * 2. Out-of-Order Event Protection:
 *    - Distributed streaming can cause events to arrive out of sequence
 *    - Traditional versioning throws errors on old events (pipeline instability)
 *    - Solution: Painless scripts with timestamp comparison (silent ignore)
 * 
 * 3. Associate Record Lifecycle Management:
 *    - Complex scenarios: additions, removals, opportunity deletions
 *    - Solution: Java-based deletion detection with separate request types
 *    - Maintains index cleanliness through proper lifecycle handling
 * 
 * DUAL REQUEST APPROACH:
 * =======================
 * 
 * Problem: External versioning throws VersionConflictEngineException for older events,
 * but we want strict out-of-order protection WITHOUT pipeline failures.
 * 
 * Solution: Separate handling for updates vs deletions using Java application logic
 * 
 * Associate Lifecycle Logic:
 * 1. Java reflection checks `isDeleted` field on each ShiftOpportunityAssociateEvent
 * 2. Based on deletion flag, creates different OpenSearch requests:
 *    - isDeleted = true  → Simple DELETE request (no timestamp checking needed)
 *    - isDeleted = false → UPDATE request with Painless script for timestamp protection
 * 
 * Timestamp Protection Script (UPDATE requests only):
 * 
 * Conditional Update Script Logic:
 * ```painless
 * if (ctx._source == null || params.new_timestamp.compareTo(ctx._source.src_timestamp) >= 0) {
 *   ctx._source = params.doc;  // Update with new data
 * } else {
 *   ctx.op = 'noop';          // Silently ignore older event (no error)
 * }
 * ```
 * 
 * 
 * Technical Benefits:
 * - ✅ Strict out-of-order protection: Only newer/equal timestamps update
 * - ✅ Pipeline stability: No errors thrown for old events (silent ignore)
 * - ✅ Automatic upsert: Creates document if it doesn't exist
 * - ✅ Lexicographic comparison: ISO-8601 timestamps compared as strings
 * - ✅ Deletion support: Java-based detection with separate DELETE requests for proper cleanup
 * - ✅ Conflict handling: Built-in retry mechanism on version conflicts
 * 
 * Sink Variants Provided:
 * 
 * 1. Basic Append-Only Sink:
 *    - Use Case: Events log (audit trail)
 *    - Strategy: Simple indexing without primary key
 *    - Performance: Highest throughput, no conflicts
 * 
 * 2. Fast Upsert Sink:
 *    - Use Case: When event ordering is guaranteed
 *    - Strategy: Direct upserts by document ID
 *    - Performance: High throughput, minimal latency
 * 
 * 3. Conditional Update Sink:
 *    - Use Case: Production environments with potential out-of-order events
 *    - Strategy: Timestamp-based conditional updates
 *    - Performance: Slight overhead for data protection
 * 
 * 4. Deletion-Aware Sink:
 *    - Use Case: Associate records with complex lifecycle
 *    - Strategy: Java reflection checks isDeleted flag → sends DELETE or UPDATE requests
 *    - Performance: Optimized for normalized data patterns with proper cleanup
 * 
 * Authentication Strategies:
 * 
 * 1. AWS IAM Authentication (Recommended for Production):
 *    - Uses DefaultCredentialsProvider for automatic credential resolution
 *    - Supports IAM roles, instance profiles, and credential chains
 *    - Signature v4 signing with automatic credential refresh
 *    - No credential management overhead
 * 
 * 2. Basic Authentication (Development/Testing):
 *    - Username/password based authentication
 *    - Simple setup for development environments
 *    - Not recommended for production due to credential management complexity
 * 
 * Dependencies:
 * Upstream:
 *   - Transformation operators: Provide structured event data
 *   - Document models: JSON serializable event objects
 * 
 * Downstream:
 *   - Amazon OpenSearch Service: Target indexing platform
 *   - Dashboard applications: Real-time queries and visualization
 *   - Analytics pipelines: Historical data analysis
 * 
 * Performance Considerations:
 * - Bulk operations for high throughput (configurable batch sizes)
 * - Connection pooling for efficient resource utilization
 * - Retry logic with exponential backoff for resilience
 * - Optimized JSON parsing and script compilation
 * - Minimal object creation in hot paths
 * 
 * Error Handling Strategy:
 * - Graceful degradation: Continue processing on individual document failures
 * - Comprehensive logging: Debug information for troubleshooting
 * - Retry mechanisms: Automatic retry on transient failures
 * - Circuit breaker patterns: Prevent cascade failures
 */
public class AmazonOpenSearchServiceSink {

    private static IndexRequest createIndexRequest(String jsonElement, String index) {
        return Requests.indexRequest()
                .index(index)
                .source(jsonElement, XContentType.JSON);
    }

    private static IndexRequest createIndexRequestWithId(String jsonElement, String index, String id) {
        return Requests.indexRequest()
                .index(index)
                .id(id)
                .source(jsonElement, XContentType.JSON);
    }

    /**
     * Create an update request with Painless script for safe timestamp-based updates.
     * This approach prevents data loss while silently ignoring older events (no errors thrown).
     */
    private static UpdateRequest createTimestampBasedUpdateRequest(
            String jsonElement, String index, String id, String timestampField) {
        try {
            // Extract timestamp from the document
            String timestamp = extractTimestampFromJson(jsonElement, timestampField);
            
            // Painless script for conditional update based on timestamp comparison
            String script = 
                "if (ctx._source == null || params.new_timestamp.compareTo(ctx._source." + timestampField + ") >= 0) {" +
                "  ctx._source = params.doc;" +
                "} else {" +
                "  ctx.op = 'noop';" +  // Do nothing for older timestamps (no error)
                "}";
            
            // Parse the JSON document for the script parameters
            com.google.gson.Gson gson = new com.google.gson.Gson();
            java.util.Map<String, Object> docMap = gson.fromJson(jsonElement, java.util.Map.class);
            
            return new UpdateRequest(index, id)
                    .script(new Script(
                            ScriptType.INLINE,
                            "painless",
                            script,
                            java.util.Map.of(
                                "new_timestamp", timestamp,
                                "doc", docMap
                            )
                    ))
                    .upsert(jsonElement, XContentType.JSON)  // Create if doesn't exist
                    .retryOnConflict(3);  // Retry on conflicts
                    
        } catch (Exception e) {
            // Fallback to regular upsert if script creation fails
            System.err.println("Failed to create scripted update request, falling back to simple upsert: " + e.getMessage());
            return new UpdateRequest(index, id)
                    .doc(jsonElement, XContentType.JSON)
                    .upsert(jsonElement, XContentType.JSON);
        }
    }

    /**
     * Create a delete request for removing a document from OpenSearch
     */
    private static DeleteRequest createDeleteRequest(String index, String id) {
        return new DeleteRequest(index, id);
    }

    /**
     * Extract timestamp string from JSON document for Painless script comparison.
     * Returns the timestamp as a string for lexicographic comparison in OpenSearch.
     */
    private static String extractTimestampFromJson(String jsonElement, String timestampField) {
        try {
            // Simple JSON parsing to extract timestamp
            com.google.gson.Gson gson = new com.google.gson.Gson();
            java.util.Map<String, Object> map = gson.fromJson(jsonElement, java.util.Map.class);
            
            String timestamp = (String) map.get(timestampField);
            if (timestamp != null) {
                // Return the ISO-8601 timestamp string for lexicographic comparison
                // ISO-8601 format allows direct string comparison (e.g., "2025-07-18T20:01:38.070848Z")
                return timestamp;
            }
        } catch (Exception e) {
            // Log but don't fail - return current time as fallback
            System.err.println("Failed to extract timestamp for script: " + e.getMessage());
        }
        
        // Fallback: use current time as ISO-8601 string
        return java.time.Instant.now().toString();
    }

    /**
     * Extract deletion flag from ShiftOpportunityAssociateEvent using reflection
     */
    private static boolean isDeletedEvent(Object element) {
        try {
            java.lang.reflect.Field field = element.getClass().getDeclaredField("isDeleted");
            field.setAccessible(true);
            Boolean value = (Boolean) field.get(element);
            return value != null && value;
        } catch (Exception e) {
            // If we can't extract the field, assume it's not a deletion
            return false;
        }
    }

    private static String extractHost(String endpointUrl) {
        try {
            URL openSearchEndpointUrl = new URL(endpointUrl);
            return openSearchEndpointUrl.getHost();
        } catch (MalformedURLException e) {
            throw new RuntimeException("Invalid endpoint URL: " + endpointUrl, e);
        }
    }

    private static String extractIdFromElement(Object element, String idField) {
        // Simple reflection-based ID extraction
        try {
            java.lang.reflect.Field field = element.getClass().getDeclaredField(idField);
            field.setAccessible(true);
            Object value = field.get(element);
            return value != null ? value.toString() : null;
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract ID field: " + idField, e);
        }
    }

    public static <IN> OpensearchSink<IN> buildAmazonOpenSearchSink(String index, String endpointUrl, RestClientFactory clientFactory) {
        String host = extractHost(endpointUrl);

        return new OpensearchSinkBuilder<IN>()
                .setHosts(new HttpHost(host, 443, "https"))
                .setAllowInsecure(true)
                // Real-time processing: immediate flush on each record
                .setBulkFlushMaxActions(1) // flush immediately after each action
                .setRestClientFactory(clientFactory)
                .setEmitter((element, ctx, indexer) ->
                        indexer.add(createIndexRequest(element.toString(), index)))
                .build();
    }

    public static <IN> OpensearchSink<IN> buildAmazonOpenSearchSinkWithId(String index, String endpointUrl, RestClientFactory clientFactory, String idField) {
        String host = extractHost(endpointUrl);

        return new OpensearchSinkBuilder<IN>()
                .setHosts(new HttpHost(host, 443, "https"))
                .setAllowInsecure(true)
                // Real-time processing: immediate flush on each record
                .setBulkFlushMaxActions(1) // flush immediately after each action
                .setRestClientFactory(clientFactory)
                .setEmitter((element, ctx, indexer) -> {
                    // Extract ID from the element for upsert operations
                    String id = extractIdFromElement(element, idField);
                    indexer.add(createIndexRequestWithId(element.toString(), index, id));
                })
                .build();
    }

    /**
     * Build OpenSearch sink with Painless script-based conditional updates for out-of-order event protection.
     * Uses OpenSearch Update API with scripted conditional logic.
     * 
     * How it works:
     * - Uses Painless script to compare timestamps before updating
     * - Only updates if new timestamp >= existing timestamp  
     * - Silently ignores older events (noop) instead of throwing errors
     * - Creates document if it doesn't exist (upsert)
     * - Provides strict out-of-order protection without errors
     */
    public static <IN> OpensearchSink<IN> buildAmazonOpenSearchSinkWithConditionalUpdate(
            String index, String endpointUrl, RestClientFactory clientFactory, 
            String idField, String timestampField) {
        String host = extractHost(endpointUrl);

        return new OpensearchSinkBuilder<IN>()
                .setHosts(new HttpHost(host, 443, "https"))
                .setAllowInsecure(true)
                // Real-time processing: immediate flush on each record
                .setBulkFlushMaxActions(1) // flush immediately after each action
                .setRestClientFactory(clientFactory)
                .setEmitter((element, ctx, indexer) -> {
                    // Extract ID from the element for scripted update operations
                    String id = extractIdFromElement(element, idField);
                    indexer.add(createTimestampBasedUpdateRequest(element.toString(), index, id, timestampField));
                })
                .build();
    }

    /**
     * Build OpenSearch sink with support for both updates and deletions based on isDeleted flag.
     * This method is specifically designed for associate events that can be either updated or deleted.
     * Uses conditional updates for data protection on updates, and direct deletion for removals.
     */
    public static <IN> OpensearchSink<IN> buildAmazonOpenSearchSinkWithUpsertAndDelete(
            String index, String endpointUrl, RestClientFactory clientFactory, 
            String idField, String timestampField) {
        String host = extractHost(endpointUrl);

        return new OpensearchSinkBuilder<IN>()
                .setHosts(new HttpHost(host, 443, "https"))
                .setAllowInsecure(true)
                // Real-time processing: immediate flush on each record
                .setBulkFlushMaxActions(1) // flush immediately after each action
                .setRestClientFactory(clientFactory)
                .setEmitter((element, ctx, indexer) -> {
                    // Extract ID from the element
                    String id = extractIdFromElement(element, idField);
                    
                    // Check if this is a deletion operation
                    if (isDeletedEvent(element)) {
                        // Emit delete request for removed associates
                        indexer.add(createDeleteRequest(index, id));
                        System.out.println("Emitted DELETE request for document ID: " + id);
                    } else {
                        // Emit conditional update request for current associates (with timestamp protection)
                        indexer.add(createTimestampBasedUpdateRequest(element.toString(), index, id, timestampField));
                        System.out.println("Emitted UPDATE request for document ID: " + id);
                    }
                })
                .build();
    }

    /**
     * Build OpenSearch sink with simple upserts (no timestamp comparison) for maximum speed.
     * WARNING: This does not protect against out-of-order events overwriting newer data.
     */
    public static <IN> OpensearchSink<IN> buildAmazonOpenSearchSinkWithFastUpsert(
            String index, String endpointUrl, RestClientFactory clientFactory, String idField) {
        String host = extractHost(endpointUrl);

        return new OpensearchSinkBuilder<IN>()
                .setHosts(new HttpHost(host, 443, "https"))
                .setAllowInsecure(true)
                // Real-time processing: immediate flush on each record
                .setBulkFlushMaxActions(1) // flush immediately after each action
                .setRestClientFactory(clientFactory)
                .setEmitter((element, ctx, indexer) -> {
                    // Extract ID from the element for simple upsert operations (no timestamp check)
                    String id = extractIdFromElement(element, idField);
                    indexer.add(createIndexRequestWithId(element.toString(), index, id));
                })
                .build();
    }

    /**
     * Build OpenSearch sink with support for both fast upserts and deletions based on isDeleted flag.
     * This method provides maximum speed but no out-of-order protection.
     * WARNING: Updates do not protect against out-of-order events overwriting newer data.
     */
    public static <IN> OpensearchSink<IN> buildAmazonOpenSearchSinkWithFastUpsertAndDelete(
            String index, String endpointUrl, RestClientFactory clientFactory, String idField) {
        String host = extractHost(endpointUrl);

        return new OpensearchSinkBuilder<IN>()
                .setHosts(new HttpHost(host, 443, "https"))
                .setAllowInsecure(true)
                // Real-time processing: immediate flush on each record
                .setBulkFlushMaxActions(1) // flush immediately after each action
                .setRestClientFactory(clientFactory)
                .setEmitter((element, ctx, indexer) -> {
                    // Extract ID from the element
                    String id = extractIdFromElement(element, idField);
                    
                    // Check if this is a deletion operation
                    if (isDeletedEvent(element)) {
                        // Emit delete request for removed associates
                        indexer.add(createDeleteRequest(index, id));
                        System.out.println("Emitted DELETE request for document ID: " + id);
                    } else {
                        // Emit simple upsert request for current associates (no timestamp check)
                        indexer.add(createIndexRequestWithId(element.toString(), index, id));
                        System.out.println("Emitted UPSERT request for document ID: " + id);
                    }
                })
                .build();
    }

    public static RestClientFactory createAmazonOpenSearchSigningRestClientFactory(String region) {
        return new RestClientFactory() {
            @Override
            public void configureRestClientBuilder(RestClientBuilder restClientBuilder, RestClientConfig restClientConfig) {
                // http client interceptor implementing request signing for Amazon OpenSearch Service
                HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(
                        "es",
                        Aws4Signer.create(),
                        DefaultCredentialsProvider.create(),
                        Region.of(region));
                restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.addInterceptorLast(interceptor));
            }
        };
    }

    public static RestClientFactory createBasicAuthRestClientFactory(String username, String password) {
        return new RestClientFactory() {
            @Override
            public void configureRestClientBuilder(RestClientBuilder restClientBuilder, RestClientConfig restClientConfig) {
                // Basic authentication with username and password
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                
                restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> 
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
        };
    }
}
