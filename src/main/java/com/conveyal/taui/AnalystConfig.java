package com.conveyal.taui;

/**
 * Represents config information for Analyst.
 */
public class AnalystConfig {
    public static final String bundleBucket = System.getenv("BUNDLE_BUCKET");
    public static final String databaseName = getEnv("DATABASE_NAME", "scenario-editor");

    public static String getEnv (String key, String defaultValue) {
        String fromEnv = System.getenv(key);

        return fromEnv != null ? fromEnv : defaultValue;
    }
}
