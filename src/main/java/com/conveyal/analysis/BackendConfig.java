package com.conveyal.analysis;

import com.conveyal.analysis.components.HttpApi;
import com.conveyal.analysis.components.LocalWorkerLauncher;
import com.conveyal.analysis.components.TaskScheduler;
import com.conveyal.analysis.components.broker.Broker;
import com.conveyal.analysis.controllers.AggregationAreaController;
import com.conveyal.analysis.controllers.BundleController;
import com.conveyal.analysis.controllers.OpportunityDatasetController;
import com.conveyal.analysis.controllers.RegionalAnalysisController;
import com.conveyal.analysis.grids.SeamlessCensusGridExtractor;
import com.conveyal.analysis.persistence.AnalysisDB;
import com.conveyal.gtfs.GTFSCache;
import com.conveyal.r5.streets.OSMCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Represents config information for the Analysis backend server.
 */
public class BackendConfig implements
        TaskScheduler.Config,
        AnalysisDB.Config,
        Broker.Config,
        BundleController.Config,
        OSMCache.Config,
        GTFSCache.Config,
        HttpApi.Config,
        RegionalAnalysisController.Config,
        AggregationAreaController.Config,
        OpportunityDatasetController.Config,
        SeamlessCensusGridExtractor.Config,
        LocalWorkerLauncher.Config
{
    private static final Logger LOG = LoggerFactory.getLogger(BackendConfig.class);

    public static final String PROPERTIES_FILE_NAME = "analysis.properties";

    protected Properties config = new Properties();
    protected Set<String> missingKeys = new HashSet<>();

    // If true, this backend instance should shut itself down after a period of inactivity.
    public final boolean autoShutdown;
    private final String bundleBucket;
    private final String databaseName;
    private final String databaseUri;
    private final String localCacheDirectory;
    private final int serverPort;
    private final boolean offline;
    private final String seamlessCensusBucket;
    private final String seamlessCensusRegion;
    private final String gridBucket;
    public final String resourcesBucket; // This appears to be unused
    public final String resultsBucket;
    private final int lightThreads;
    private final int heavyThreads;
    private final int maxWorkers;

    // For use in testing - setting this field will activate alternate code paths that cause intentional failures.
    private boolean testTaskRedelivery = false;

    public BackendConfig () {
        // Use default non-testing properties file.
        this(PROPERTIES_FILE_NAME);
    }

    public BackendConfig (String filename) {
        try {
            FileInputStream is = new FileInputStream(filename);
            config.load(is);
            is.close();
        } catch (Exception e) {
            String message = "Could not read config file " + filename;
            LOG.error(message);
            throw new RuntimeException(message, e);
        }

        // We intentionally don't supply any defaults here - any 'defaults' should be shipped in an example config file.
        autoShutdown = Boolean.parseBoolean(getProperty("auto-shutdown", false));
        bundleBucket = getProperty("bundle-bucket", true);
        databaseName = getProperty("database-name", true);
        databaseUri = getProperty("database-uri", false);
        localCacheDirectory = getProperty("local-cache", true);
        serverPort = Integer.parseInt(getProperty("server-port", true));
        offline = Boolean.parseBoolean(getProperty("offline", true));
        seamlessCensusBucket = getProperty("seamless-census-bucket", true);
        seamlessCensusRegion = getProperty("seamless-census-region", true);
        gridBucket = getProperty("grid-bucket", true);
        resourcesBucket = getProperty("resources-bucket", true);
        resultsBucket = getProperty("results-bucket", true);
        lightThreads = Integer.parseInt(getProperty("light-threads", true));
        heavyThreads = Integer.parseInt(getProperty("heavy-threads", true));
        maxWorkers = Integer.parseInt(getProperty("max-workers", true));
        if (!missingKeys.isEmpty()) {
            LOG.error("You must provide these configuration properties: {}", String.join(", ", missingKeys));
            System.exit(1);
        }
    }

    private String getProperty (String key, boolean require) {
        String value = config.getProperty(key);
        if (require && value == null) {
            LOG.error("Missing configuration option {}", key);
            missingKeys.add(key);
        }
        return value;
    }

    // Implementations of Component and HttpController Config interfaces
    // Note that one method can implement several Config interfaces at once.

    @Override public int lightThreads () { return lightThreads; }
    @Override public int heavyThreads () { return heavyThreads; }
    @Override public String databaseUri () { return databaseUri; }
    @Override public String databaseName () { return databaseName; }
    @Override public String resultsBucket () { return resultsBucket; }
    @Override public boolean testTaskRedelivery () { return testTaskRedelivery; }
    @Override public String gridBucket () { return gridBucket; }
    @Override public String seamlessCensusRegion () { return seamlessCensusRegion; }
    @Override public String seamlessCensusBucket () { return seamlessCensusBucket; }
    @Override public int serverPort () { return serverPort; }
    @Override public String localCacheDirectory () { return localCacheDirectory;}
    @Override public String bundleBucket () { return bundleBucket; }
    @Override public boolean offline () { return offline; }
    @Override public int maxWorkers () { return maxWorkers; }

}
