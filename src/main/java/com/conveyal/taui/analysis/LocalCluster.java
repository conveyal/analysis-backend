package com.conveyal.taui.analysis;

import com.conveyal.gtfs.BaseGTFSCache;
import com.conveyal.osmlib.OSMCache;
import com.conveyal.r5.analyst.broker.Broker;
import com.conveyal.r5.analyst.broker.BrokerMain;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
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
public class LocalCluster {

    private static final Logger LOG = LoggerFactory.getLogger(LocalCluster.class);
    public final int brokerPort;
    public Thread brokerThread;
    public List<Thread> workerThreads = new ArrayList<>();
    public Broker broker;

    /**
     * This used to start up a separate broker thread. This is no longer necessary because the broker actions are
     * just performed in HTTP handler threads.
     * @param nWorkers cannot currently start more than 1 worker because the IDs are static, see AnalystWorker.machineId
     */
    public LocalCluster(int brokerPort, BaseGTFSCache gtfsCache, OSMCache osmCache, int nWorkers) {

        this.brokerPort = brokerPort;

        Properties workerConfig = new Properties();

        workerConfig.setProperty("auto-shutdown", "false"); // cause that would be annoying
        workerConfig.setProperty("work-offline", "true");
        workerConfig.setProperty("broker-address", "localhost");
        workerConfig.setProperty("broker-port", "" + brokerPort);
        workerConfig.setProperty("cache-dir", AnalysisServerConfig.localCache);
        workerConfig.setProperty("pointsets-bucket", AnalysisServerConfig.gridBucket);

        TransportNetworkCache transportNetworkCache = new TransportNetworkCache(gtfsCache, osmCache);
        for (int i = 0; i < nWorkers; i++) {
            AnalystWorker worker = new AnalystWorker(workerConfig, transportNetworkCache);
            Thread workerThread = new Thread(worker, "WORKER " + worker.machineId);
            workerThreads.add(workerThread);
            workerThread.start();
        }

    }
}
