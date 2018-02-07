package com.conveyal.taui;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Represents config information for the Analysis backend server.
 * FIXME Gets some info from a config file and some from environment variables.
 * Everything should come from one place.
 */
public class AnalysisServerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(AnalysisServerConfig.class);
    private static Properties config = new Properties();

    static {
        try {
            FileInputStream is = new FileInputStream("application.conf");
            config.load(is);
            is.close();
        } catch (Exception e) {
            LOG.warn("Could not read config file. If all variables are in the environment, ignore this message.");
        }
    }

    public static final String bundleBucket = getEnv("BUNDLE_BUCKET", null);
    public static final String databaseName = getEnv("DATABASE_NAME", "scenario-editor");
    public static final String databaseUri = getEnv("MONGOLAB_URI", null);
    public static final String auth0ClientId = getEnv("AUTH0_CLIENT_ID", null);
    public static final byte[] auth0Secret = new Base64(true).decode(getEnv("AUTH0_SECRET", null));
    public static final String localCache = getEnv("LOCAL_CACHE", "cache");
    public static final String assetLocation = getEnv("ASSET_LOCATION", "https://d1uqjuy3laovxb.cloudfront.net");
    public static final int port = Integer.parseInt(getEnv("PORT", "7070"));
    public static final boolean offline = Boolean.parseBoolean(getEnv("OFFLINE", "false"));
    public static final String brokerUrl = getEnv("BROKER_URL", "http://localhost:7070");
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

    static {
        if (!offline && (brokerUrl == null || bundleBucket == null || auth0ClientId == null || auth0Secret == null || gridBucket == null || resultsBucket == null || resultsQueue == null)) {
            LOG.error("Application is missing config variables needed in online mode.");
        }
    }
}
