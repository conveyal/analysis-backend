package com.conveyal.taui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Represents config information for Analyst.
 */
public class AnalystConfig {
    private static final Logger LOG = LoggerFactory.getLogger(AnalystConfig.class);
    private static Properties config = new Properties();

    static {
        try {
            FileInputStream is = new FileInputStream("application.conf");
            config.load(is);
            is.close();
        } catch (IOException e) {
            LOG.info("Could not read config file. If all variables are in the environment, ignore this message", e);
        }
    }

    public static final String bundleBucket = getEnv("BUNDLE_BUCKET", null);
    public static final String databaseName = getEnv("DATABASE_NAME", "scenario-editor");
    public static final String databaseUri = getEnv("MONGOLAB_URI", null);
    public static final String auth0ClientId = getEnv("AUTH0_CLIENT_ID", null);
    public static final String auth0Secret = getEnv("AUTH0_SECRET", null);
    public static final String localCache = getEnv("LOCAL_CACHE", "cache");
    public static final String assetLocation = getEnv("ASSET_LOCATION", "https://d1uqjuy3laovxb.cloudfront.net");
    public static final int port = Integer.parseInt(getEnv("PORT", "7070"));
    public static final boolean offline = Boolean.parseBoolean(getEnv("OFFLINE", "false"));
    public static final String brokerUrl = getEnv("BROKER_URL", "http://localhost:9001");
    public static final String vexUrl = getEnv("VEX_URL", "http://osm.conveyal.com/vex");
    public static final String seamlessCensusBucket = getEnv("SEAMLESS_CENSUS_BUCKET", "lodes-data-2014");
    public static final String gridBucket = getEnv("GRID_BUCKET", null);
    public static final String resultsBucket = getEnv("RESULTS_BUCKET", null);
    public static final String resultsQueue = getEnv("RESULTS_QUEUE", null);
    public static final String region = getEnv("REGION", "eu-west-1");

    public static String getEnv (String key, String defaultValue) {
        String val = System.getenv(key);

        if (val == null) {
            val = config.getProperty(key);
        }

        return val != null ? val : defaultValue;
    }
}
