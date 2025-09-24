package com.amazon.shift.opportunity;

/**
 * Real-Time Shift Opportunity Stream Processing Application
 * ========================================================
 * 
 * Author: Saahil Sanganeriya
 * 
 * Description:
 * This Apache Flink application processes real-time shift opportunity events from Amazon Kinesis Data Streams
 * and transforms them into structured data for storage in Amazon OpenSearch Service. The application implements
 * a dual-sink pattern with current state management and complete audit logging capabilities.
 * 
 * Key Features:
 * - Real-time event processing with out-of-order event protection
 * - Dual-sink pattern: current state (upsert) + audit trail (append-only)
 * - Associate expansion for detailed analytics
 * - S3 backup storage with time-based partitioning
 * - Configurable processing modes (fast vs conditional updates)
 * - AWS IAM authentication for OpenSearch integration
 * 
 * Data Flow:
 * 1. SNS → Lambda → Kinesis Data Streams (event ingestion)
 * 2. Kinesis → Flink (this application) → OpenSearch Service (real-time processing)
 * 3. Flink → S3 (backup storage)
 * 
 * Dependencies:
 * Upstream:
 *   - Amazon Kinesis Data Streams: ShiftOpportunityServiceEvents
 *   - Event structure: SNS envelope with shift opportunity business data
 * 
 * Downstream:
 *   - Amazon OpenSearch Service indexes:
 *     - shift-opportunity-current (upsert mode)
 *     - shift-opportunity-events-log (append mode) 
 *     - shift-opportunity-associates (upsert with delete support)
 *   - Amazon S3: backup storage with time-based partitioning
 * 
 * Configuration Parameters:
 *   - InputStreamArn: ARN of the Kinesis Data Stream
 *   - OpenSearchEndpoint: OpenSearch cluster endpoint URL
 *   - OpenSearchUsername/Password: Basic auth credentials (optional, IAM preferred)
 *   - Region: AWS region for IAM authentication
 *   - ProcessingMode: "fast" (no out-of-order protection) vs "conditional" (timestamp-based protection)
 *   - S3BucketName/S3Prefix: S3 backup configuration (optional)
 *   - Index names for OpenSearch targets (configurable with defaults)
 * 
 * Architecture Decisions:
 * - Java + DataStream API: Maximum flexibility and feature completeness vs Python Table API
 * - Custom OpenSearch sink: Required for AWS IAM authentication with Signature v4 signing
 * - Dual-sink pattern: Supports both real-time querying and historical analysis
 * - Out-of-order protection: Configurable timestamp-based conditional updates using Painless scripts
 * 
 * Performance Characteristics:
 * - Target latency: Sub-second end-to-end (Kinesis → OpenSearch)
 * - Throughput: Thousands of events per hour with burst capability
 * - Checkpointing: 10-second intervals for fault tolerance
 * - Auto-scaling: Enabled based on CPU utilization
 * 
 */

import com.amazon.shift.opportunity.serialization.EventDeserializationSchema;
import com.amazon.shift.opportunity.model.ShiftOpportunityCurrentEvent;
import com.amazon.shift.opportunity.model.ShiftOpportunityLogEvent;
import com.amazon.shift.opportunity.model.ShiftOpportunityAssociateEvent;
import com.amazon.shift.opportunity.model.ShiftOpportunityEvent;
import com.amazon.shift.opportunity.transform.*;
import com.amazon.shift.opportunity.sink.*;
import com.amazon.shift.opportunity.util.ParameterToolUtils;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.opensearch.sink.RestClientFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;


/**
 * Main Flink streaming application class for processing shift opportunity events.
 * 
 * This class orchestrates the entire data pipeline from Kinesis source to multiple OpenSearch sinks,
 * implementing a production-ready streaming architecture with fault tolerance and scalability.
 */
public class ProcessShiftOpportunityStream {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessShiftOpportunityStream.class);

    /**
     * Main application entry point.
     * 
     * Sets up the Flink execution environment, configures sources and sinks, defines transformations,
     * and executes the streaming job. Supports both local development and production deployment
     * on Amazon Managed Service for Apache Flink.
     * 
     * @param args Command line arguments (used only in local development mode)
     * @throws Exception If application setup or execution fails
     */
    public static void main(String[] args) throws Exception {
        // Initialize Flink streaming execution environment
        // This automatically detects whether running locally or in managed service
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ===========================================
        // CONFIGURATION PARAMETER LOADING
        // ===========================================
        
        /*
         * Configuration handling supports two deployment modes:
         * 1. Local development: Parameters passed via command line arguments
         * 2. Production (MSF): Parameters loaded from application properties
         * 
         * This dual approach enables seamless development-to-production workflow
         */
        ParameterTool parameter;
        if (env instanceof LocalStreamEnvironment) {
            // Local development mode: Use command line arguments
            // Example: --InputStreamArn arn:<partition>:kinesis:us-east-1:123:stream/test
            parameter = ParameterTool.fromArgs(args);
            LOG.info("Running in LOCAL mode - using command line parameters");
        } else {
            // Production mode: Load from Kinesis Analytics application properties
            // These are configured through the MSF console or CloudFormation
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

            Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

            if (flinkProperties == null) {
                throw new RuntimeException("Unable to load FlinkApplicationProperties properties from the MSF (Kinesis Analytics) runtime.");
            }

            parameter = ParameterToolUtils.fromApplicationProperties(flinkProperties);
            LOG.info("Running in PRODUCTION mode - using MSF application properties");
        }

        // ===========================================
        // KINESIS SOURCE CONFIGURATION
        // ===========================================
        
        /*
         * Configure Kinesis Data Streams as the primary data source.
         * Events arrive as JSON payloads wrapped in SNS envelopes, processed by upstream Lambda.
         * 
         * Key considerations:
         * - Stream ARN must be provided as configuration parameter
         * - Custom deserialization handles complex nested JSON structure
         * - Watermarking strategy allows for 5-second out-of-order tolerance
         * - Event time extraction uses source system timestamp for accurate processing
         */

        // Extract and validate required Kinesis stream configuration
        String streamArn = parameter.get("InputStreamArn");
        Preconditions.checkNotNull(streamArn, "InputStreamArn configuration parameter not defined");
        LOG.info("Configuring Kinesis source for stream: {}", streamArn);

        // Build Kinesis source with custom deserialization schema
        // EventDeserializationSchema handles parsing of complex SNS-wrapped JSON events
        KinesisStreamsSource<ShiftOpportunityEvent> kinesisSource = KinesisStreamsSource.<ShiftOpportunityEvent>builder()
                .setStreamArn(streamArn)
                .setDeserializationSchema(new EventDeserializationSchema())
                .build();

        // Create data stream with event-time processing and watermark strategy
        DataStream<ShiftOpportunityEvent> kinesisStream = env.fromSource(
                        kinesisSource,
                        /*
                         * Watermark Strategy Configuration:
                         * - BoundedOutOfOrderness: Allows events to arrive up to 5 seconds late
                         * - Timestamp Assigner: Uses srcTimestamp (source system timestamp) for event time
                         * - Fallback: Uses processing time if source timestamp is missing
                         * 
                         * This strategy balances latency with out-of-order tolerance for real-time processing
                         */
                        WatermarkStrategy.<ShiftOpportunityEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> 
                                    event.srcTimestamp != null ? event.srcTimestamp.toEpochMilli() : System.currentTimeMillis()),
                        "Kinesis source")
                .returns(ShiftOpportunityEvent.class);

        // ===========================================
        // DATA TRANSFORMATIONS
        // ===========================================
        
        /*
         * Transform raw Kinesis events into multiple specialized output formats:
         * 1. Current State: Latest state per opportunity (upsert pattern)
         * 2. Audit Log: Complete event history (append-only pattern)
         * 3. Associate Records: Expanded view with one record per associate assignment
         * 
         * Each transformation serves different analytical and operational use cases
         */

        // Data Quality: Filter out invalid events
        // Only process events with valid opportunity IDs to ensure data integrity
        DataStream<ShiftOpportunityEvent> validEvents = kinesisStream
                .filter(event -> event.opportunityId != null && !event.opportunityId.isEmpty());
        LOG.info("Applied data quality filter for valid opportunity IDs");

        // Transformation 1: Current State Events
        // Maps each event to current state format for real-time dashboard queries
        // Includes derived fields like claim_status and fill_status for business logic
        DataStream<ShiftOpportunityCurrentEvent> currentEvents = validEvents
                .map(new ShiftOpportunityToCurrentEvent());

        // Transformation 2: Audit Log Events  
        // Preserves complete event history with metadata for compliance and debugging
        // Includes change tracking and previous state information
        DataStream<ShiftOpportunityLogEvent> logEvents = validEvents
                .map(new ShiftOpportunityToLogEvent());

        // Transformation 3: Associate Event Expansion
        // Creates individual records for each accepted associate ID
        // Enables detailed analytics on associate assignment patterns
        // Uses flatMap to handle one-to-many relationship (1 opportunity → N associates)
        DataStream<ShiftOpportunityAssociateEvent> associateEvents = validEvents
                .flatMap(new ShiftOpportunityToAssociateEvents());

        // ===========================================
        // OPENSEARCH SINK CONFIGURATION
        // ===========================================
        
        /*
         * Configure Amazon OpenSearch Service as the primary sink for real-time queries and dashboards.
         * Implements dual-sink pattern with three distinct indexes:
         * 
         * 1. Current State Index: Upsert pattern for latest opportunity state
         * 2. Events Log Index: Append-only pattern for complete audit trail  
         * 3. Associates Index: Normalized view for associate-level analytics
         * 
         * Authentication supports both IAM (production) and basic auth (development)
         */

        // Extract and validate OpenSearch endpoint configuration
        String opensearchEndpoint = parameter.get("OpenSearchEndpoint");
        Preconditions.checkNotNull(opensearchEndpoint, "OpenSearchEndpoint configuration parameter not defined");
        LOG.info("Configuring OpenSearch endpoint: {}", opensearchEndpoint);

        // Authentication configuration (prioritizes IAM over basic auth)
        String opensearchUsername = parameter.get("OpenSearchUsername");
        String opensearchPassword = parameter.get("OpenSearchPassword");

        // Index name configuration with sensible defaults
        String currentStateIndex = parameter.get("OpenSearchCurrentStateIndex", "shift-opportunity-current");
        String eventsLogIndex = parameter.get("OpenSearchEventsLogIndex", "shift-opportunity-events-log");
        String associatesIndex = parameter.get("OpenSearchAssociatesIndex", "shift-opportunity-associates");
        
        LOG.info("OpenSearch indexes - Current: {}, Log: {}, Associates: {}", 
                currentStateIndex, eventsLogIndex, associatesIndex);

        // Authentication Strategy Selection
        RestClientFactory osRestClientFactory;
        if (opensearchUsername != null && opensearchPassword != null) {
            // Development/Testing: Basic authentication
            // Not recommended for production due to credential management complexity
            osRestClientFactory = AmazonOpenSearchServiceSink.createBasicAuthRestClientFactory(opensearchUsername, opensearchPassword);
            LOG.info("Using basic authentication for OpenSearch");
        } else {
            // Production: AWS IAM authentication with Signature v4 signing
            // Leverages IAM roles and policies for secure, credential-free access
            String region = parameter.get("Region", "us-east-1");
            osRestClientFactory = AmazonOpenSearchServiceSink.createAmazonOpenSearchSigningRestClientFactory(region);
            LOG.info("Using AWS IAM authentication for OpenSearch in region: {}", region);
        }

        // ===========================================
        // OPENSEARCH SINK CREATION & PROCESSING MODE
        // ===========================================
        
        /*
         * Processing Mode Configuration:
         * 
         * CONDITIONAL (default, recommended for production):
         * - Timestamp-based conditional updates using Painless scripts
         * - Prevents data regression from out-of-order events
         * - Slightly higher latency due to timestamp comparison
         * - Ensures data consistency in distributed processing
         * 
         * FAST (maximum throughput):
         * - Direct upserts without timestamp validation
         * - Maximum processing speed and throughput
         * - Risk of data regression if events arrive out-of-order
         * - Suitable for scenarios where event ordering is guaranteed
         */
        
        String processingMode = parameter.get("ProcessingMode", "conditional");
        LOG.info("Processing mode configured: {} (conditional=safe, fast=maximum-speed)", processingMode);
        
        // SINK 1: Current State Index (Upsert Pattern)
        // Maintains latest state per opportunity for real-time dashboards
        if ("fast".equalsIgnoreCase(processingMode)) {
            LOG.info("Current state sink: FAST mode - maximum speed, no out-of-order protection");
            currentEvents.sinkTo(AmazonOpenSearchServiceSink.buildAmazonOpenSearchSinkWithFastUpsert(
                    currentStateIndex, opensearchEndpoint, osRestClientFactory, "opportunityId"));
        } else {
            LOG.info("Current state sink: CONDITIONAL mode - timestamp-based updates, out-of-order protection");
            // Uses Painless script to compare src_timestamp before updating
            // Prevents older events from overwriting newer opportunity state
            currentEvents.sinkTo(AmazonOpenSearchServiceSink.buildAmazonOpenSearchSinkWithConditionalUpdate(
                    currentStateIndex, opensearchEndpoint, osRestClientFactory, "opportunityId", "src_timestamp"));
        }

        // SINK 2: Events Log Index (Append-Only Pattern)
        // Preserves complete audit trail for compliance and debugging
        // No primary key specified - all events are appended regardless of duplicates
        logEvents.sinkTo(AmazonOpenSearchServiceSink.buildAmazonOpenSearchSink(
                eventsLogIndex, opensearchEndpoint, osRestClientFactory));
        LOG.info("Events log sink: Append-only mode for complete audit trail");

        // SINK 3: Associates Index (Normalized Pattern with Deletion Support)
        // One record per (opportunity_id, associate_id) combination
        // Supports both assignment additions and removals for accurate associate tracking
        if ("fast".equalsIgnoreCase(processingMode)) {
            LOG.info("Associates sink: FAST mode - maximum speed with deletion support");
            associateEvents.sinkTo(AmazonOpenSearchServiceSink.buildAmazonOpenSearchSinkWithFastUpsertAndDelete(
                    associatesIndex, opensearchEndpoint, osRestClientFactory, "documentId"));
        } else {
            LOG.info("Associates sink: CONDITIONAL mode - timestamp-based updates with deletion support");
            // Composite key handling with timestamp-based versioning
            // Prevents out-of-order events from corrupting associate assignment history
            associateEvents.sinkTo(AmazonOpenSearchServiceSink.buildAmazonOpenSearchSinkWithUpsertAndDelete(
                    associatesIndex, opensearchEndpoint, osRestClientFactory, "documentId", "src_timestamp"));
        }

        // ===========================================
        // S3 BACKUP SINK (OPTIONAL)
        // ===========================================
        
        /*
         * Optional S3 sink for long-term data retention and backup.
         * Provides disaster recovery capability and enables batch analytics on historical data.
         * 
         * Rolling Policy Configuration:
         * - Time-based: New files every 15 minutes for manageable file sizes
         * - Inactivity-based: Close files after 5 minutes of no data
         * - Size-based: Maximum 128MB per file for optimal S3 performance
         * 
         * Only activated if S3BucketName parameter is provided
         */

        String s3BucketName = parameter.get("S3BucketName");
        String s3Prefix = parameter.get("S3Prefix", "shift-opportunity-logs");

        if (s3BucketName != null && !s3BucketName.isEmpty()) {
            LOG.info("Configuring S3 backup sink for bucket: s3://{}/{}", s3BucketName, s3Prefix);
            
            // Transform events to JSON format for S3 storage
            // Uses toString() method which leverages Jackson serialization
            DataStream<String> logEventsAsJson = logEvents.map(event -> event.toString());

            // Configure S3 sink with optimized rolling policy
            FileSink<String> s3Sink = FileSink
                    .forRowFormat(new Path("s3://" + s3BucketName + "/" + s3Prefix), 
                                 new SimpleStringEncoder<String>("UTF-8"))
                    .withBucketCheckInterval(60000) // Check for new buckets every minute
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    // Time-based rolling: Balance between file size and frequency
                                    .withRolloverInterval(Duration.ofMinutes(15))
                                    // Inactivity rolling: Ensure timely data availability
                                    .withInactivityInterval(Duration.ofMinutes(5))
                                    // Size-based rolling: Optimize for S3 performance
                                    .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                    .build())
                    .build();

            logEventsAsJson.sinkTo(s3Sink).name("S3 Log Sink");
            LOG.info("S3 backup sink activated - files will be partitioned by time and size");
        } else {
            LOG.info("S3BucketName not configured, skipping S3 backup sink");
        }

        // ===========================================
        // PIPELINE SUMMARY & EXECUTION
        // ===========================================
        
        // Log pipeline configuration summary for operational visibility
        LOG.info("=== Shift Opportunity Stream Processing Pipeline ===");
        LOG.info("Source: Kinesis stream {}", streamArn);
        LOG.info("Processing mode: {}", processingMode);
        LOG.info("Current state → OpenSearch {}/{}", opensearchEndpoint, currentStateIndex);
        LOG.info("Events log → OpenSearch {}/{}", opensearchEndpoint, eventsLogIndex);
        LOG.info("Associates → OpenSearch {}/{}", opensearchEndpoint, associatesIndex);
        if (s3BucketName != null && !s3BucketName.isEmpty()) {
            LOG.info("Backup → S3 s3://{}/{}", s3BucketName, s3Prefix);
        }
        LOG.info("================================================");

        // Execute the streaming job
        // This call blocks until the job is cancelled or fails
        env.execute("Shift Opportunity Stream Processing");
    }
}