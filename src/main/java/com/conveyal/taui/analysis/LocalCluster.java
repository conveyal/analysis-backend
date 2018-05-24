package com.conveyal.taui.analysis;

import com.conveyal.gtfs.BaseGTFSCache;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
import com.conveyal.r5.streets.OSMCache;
import com.conveyal.r5.transit.TransportNetworkCache;
import com.conveyal.taui.AnalysisServerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Start up all the components of an analyst cluster locally.
 */
public abstract class LocalCluster {

    /**
     * This used to start up a separate broker thread. This is no longer necessary because the broker actions are
     * just performed in HTTP handler threads.
     * @param nWorkers cannot currently start more than 1 worker because the IDs are static, see AnalystWorker.machineId
     */
    public static List<Thread> start (BaseGTFSCache gtfsCache, OSMCache osmCache, int nWorkers) {

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

        if (AnalysisServerConfig.testTaskRedelivery) {
            // When testing we want multiple workers,
            // which will be set to not conflict with one another despite being on one machine.
            nWorkers = 4;
            // Tell the workers to return fake results, but fail part of the time.
            workerConfig.setProperty("test-task-redelivery", "true");
        }

        TransportNetworkCache transportNetworkCache = new TransportNetworkCache(gtfsCache, osmCache);
        for (int i = 0; i < nWorkers; i++) {
            AnalystWorker worker = new AnalystWorker(workerConfig, transportNetworkCache);
            Thread workerThread = new Thread(worker, "WORKER " + worker.machineId);
            workerThreads.add(workerThread);
            workerThread.start();
        }

        return workerThreads;
    }
}
