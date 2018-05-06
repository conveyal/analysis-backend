package com.conveyal.taui;

import com.amazonaws.services.ec2.model.InstanceType;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Represents config information for the Analysis backend server.
 * FIXME This should not be all static.
 */
public abstract class AnalysisServerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(AnalysisServerConfig.class);

    private static Properties config = new Properties();

    private static Set<String> missingKeys = new HashSet<>();

    static {
        try {
            FileInputStream is = new FileInputStream("analysis.properties.tmp");
            config.load(is);
            is.close();
        } catch (Exception e) {
            LOG.error("Could not read config file 'analysis.properties'.");
        }
    }

    // We intentionally don't supply any defaults here - any 'defaults' should be shipped in an example config file.
    // FIXME this is so so much static...
    public static final String serverAddress = getProperty("server-address", true);
    public static final String bundleBucket = getProperty("bundle-bucket", true);
    public static final String databaseName = getProperty("database-name", true);
    public static final String databaseUri = getProperty("database-uri", false);
    public static final String auth0ClientId = getProperty("auth0-client-id", false);
    public static final byte[] auth0Secret = new Base64(true).decode(getProperty("auth0-secret", false));
    public static final String localCacheDirectory = getProperty("local-cache", true);
    public static final String frontendUrl = getProperty("frontend-url", true);
    public static final int serverPort = Integer.parseInt(getProperty("server-port", true));
    public static final boolean offline = Boolean.parseBoolean(getProperty("offline", true));
    public static final String vexUrl = getProperty("vex-url", true);
    public static final String seamlessCensusBucket = getProperty("seamless-census-bucket", true);
    public static final String gridBucket = getProperty("grid-bucket", true);
    public static final String resultsBucket = getProperty("results-bucket", true);
    public static final String awsRegion = getProperty("aws-region", true);
    public static final String workerLogGroup = getProperty("worker-log-group", true);
    public static final int maxThreads = Integer.parseInt(getProperty("max-threads", true));
    public static final int maxWorkers = Integer.parseInt(getProperty("max-workers", true));

    // AWS specific stuff. This should be moved to another config object when we make this portable to other environments.
    public static final int workerPort = Integer.parseInt(getProperty("worker-port", true));
    public static final String workerAmiId = getProperty("worker-ami-id", true);
    public static final String workerSubnetId = getProperty("worker-subnet-id", true);
    public static final String workerIamRole = getProperty("worker-iam-role", true);
    public static final InstanceType workerInstanceType = InstanceType.valueOf(getProperty("worker-type", true));

    private static String getProperty (String key, boolean require) {
        String value = config.getProperty(key);
        if (require && value == null) {
            LOG.error("Missing configuration option {}", key);
            missingKeys.add(key);
        }
        return value;
    }

    static {
        if (!offline && (bundleBucket == null || auth0ClientId == null || auth0Secret == null || gridBucket == null || resultsBucket == null || workerLogGroup == null)) {
            LOG.error("Application is missing config variables needed in online mode.");
        }
        if (!missingKeys.isEmpty()) {
            LOG.error("You must provide these configuration properties: {}", String.join(", ", missingKeys));
            System.exit(1);
        }
    }
}
