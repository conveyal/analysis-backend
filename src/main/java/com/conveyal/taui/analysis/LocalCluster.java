package com.conveyal.taui.analysis;

import com.conveyal.gtfs.BaseGTFSCache;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
import com.conveyal.r5.streets.OSMCache;
import com.conveyal.r5.transit.TransportNetworkCache;
import com.conveyal.taui.AnalysisServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Start up all the components of an analyst cluster locally.
 */
public abstract class LocalCluster {

    private static final Logger LOG = LoggerFactory.getLogger(LocalCluster.class);

    private static final int N_WORKERS_LOCAL = 1;

    private static final int N_WORKERS_LOCAL_TESTING = 4;

    /**
     * This used to start up a separate broker thread. This is no longer necessary because the broker actions are
     * just performed in HTTP handler threads.
     * Note that starting more than one worker on the same machine will not improve throughput. Each worker tries to
     * use all available cores on its machine. Running multiple workers is just for testing cluster behavior.
     * Only one of the workers will be able to receive single point messages since they listen on a specific port.
     */
    public static List<Thread> start (BaseGTFSCache gtfsCache, OSMCache osmCache) {

        List<Thread> workerThreads = new ArrayList<>();
        Properties workerConfig = new Properties();

        // Do not auto-shutdown the local machine
        workerConfig.setProperty("auto-shutdown", "false");
        workerConfig.setProperty("work-offline", "true");
        workerConfig.setProperty("broker-address", "localhost");
        workerConfig.setProperty("broker-port", Integer.toString(AnalysisServerConfig.serverPort));
        workerConfig.setProperty("cache-dir", AnalysisServerConfig.localCacheDirectory);
        workerConfig.setProperty("pointsets-bucket", AnalysisServerConfig.gridBucket);
        workerConfig.setProperty("aws-region", AnalysisServerConfig.awsRegion);

        // From a throughput perspective there is no point in running more than one worker locally, since each worker
        // has at least as many threads as there are processor cores. But for testing purposes (e.g. testing that task
        // redelivery works right) we want to start more workers to simulate running on a cluster.
        int nWorkers = N_WORKERS_LOCAL;
        if (AnalysisServerConfig.testTaskRedelivery) {
            // When testing we want multiple workers. Below, all but one will have single point listening disabled
            // to allow them to run on the same machine without port conflicts.
            nWorkers = N_WORKERS_LOCAL_TESTING;
            // Tell the workers to return fake results, but fail part of the time.
            workerConfig.setProperty("test-task-redelivery", "true");
        }

        TransportNetworkCache transportNetworkCache = new TransportNetworkCache(gtfsCache, osmCache);
        LOG.info("Starting {} workers locally.", nWorkers);
        for (int i = 0; i < nWorkers; i++) {
            // Avoid starting more than one worker on the same machine trying to listen on the same port.
            if (i > 0) {
                workerConfig.setProperty("listen-for-single-point", "false");
            }
            AnalystWorker worker = new AnalystWorker(workerConfig, transportNetworkCache);
            Thread workerThread = new Thread(worker, "WORKER " + i);
            workerThreads.add(workerThread);
            workerThread.start();
            // Note that machineId is static, so all workers have the same machine ID for now. This should be fixed somehow.
            LOG.info("Started worker {} with machine ID {}.", i, worker.machineId);
        }

        return workerThreads;
    }
}
