package com.amazon.shift.opportunity.util;

/**
 * Parameter Tool Utility Class
 * ============================
 * 
 * Author: Saahil Sanganeriya
 * Role: Utility for converting application properties to Flink ParameterTool
 * 
 * Description:
 * This utility class provides a bridge between Amazon Managed Service for Apache Flink
 * application properties and Flink's ParameterTool. It handles the conversion from
 * Java Properties objects (as returned by MSF runtime) to the Map-based format
 * required by Flink's parameter handling system.
 * 
 * Key Features:
 * - Type-safe conversion from Properties to ParameterTool
 * - Optimized for production MSF deployment scenarios
 * - Thread-safe static utility methods
 * - Memory-efficient conversion with pre-sized HashMap
 * 
 * MSF Integration:
 * In Amazon Managed Service for Apache Flink, application properties are provided
 * through the KinesisAnalyticsRuntime.getApplicationProperties() method which returns
 * a Map<String, Properties>. This utility converts the Properties objects into
 * Flink's standard ParameterTool format for consistent parameter access throughout
 * the application.
 */

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for converting between different parameter format representations.
 * 
 * Provides static methods for bridging Amazon MSF application properties with
 * Flink's standard parameter handling mechanisms.
 */
public class ParameterToolUtils {
    
    /**
     * Converts Java Properties to Flink ParameterTool.
     * 
     * This method handles the conversion from the Properties format used by
     * Amazon Managed Service for Apache Flink to the Map-based ParameterTool
     * format expected by Flink applications.
     * 
     * @param properties Properties object from MSF application configuration
     * @return ParameterTool instance for consistent parameter access
     * @throws ClassCastException if Properties contain non-String keys or values
     */
    public static ParameterTool fromApplicationProperties(Properties properties) {
        // Pre-size HashMap for optimal memory usage and performance
        Map<String, String> map = new HashMap<>(properties.size());

        // Convert Properties to Map using efficient lambda iteration
        // Type-safe casting assumes MSF properties are String key-value pairs
        properties.forEach((k, v) -> map.put((String) k, (String) v));

        // Create ParameterTool from the converted Map
        return ParameterTool.fromMap(map);
    }
}
