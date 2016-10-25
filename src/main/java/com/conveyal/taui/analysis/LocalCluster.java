package com.conveyal.taui.analysis;

import com.conveyal.r5.analyst.broker.Broker;
import com.conveyal.r5.analyst.broker.BrokerMain;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
import com.conveyal.taui.AnalystConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Start up all the components of an analyst cluster locally.
 */
public class LocalCluster {
    private static final Logger LOG = LoggerFactory.getLogger(LocalCluster.class);

    public final int brokerPort;

    public LocalCluster(int brokerPort) {
        this.brokerPort = brokerPort;

        // start the broker
        Properties brokerConfig = new Properties();
        // I believe work-offline tells the broker not to spin up AWS instances.
        brokerConfig.setProperty("work-offline", "true");
        brokerConfig.setProperty("bind-address", "localhost");
        brokerConfig.setProperty("port", "" + brokerPort);

        BrokerMain broker = new BrokerMain(brokerConfig);

        new Thread(broker::run).start();

        Properties workerConfig = new Properties();

        workerConfig.setProperty("auto-shutdown", "false"); // cause that would be annoying
        workerConfig.setProperty("work-offline", "true");
        workerConfig.setProperty("broker-address", "localhost");
        workerConfig.setProperty("broker-port", "" + brokerPort);
        workerConfig.setProperty("cache-dir", AnalystConfig.localCache);
        workerConfig.setProperty("pointsets-bucket", AnalystConfig.gridBucket);

        AnalystWorker worker = new AnalystWorker(workerConfig);

        new Thread(worker::run).start();
    }
}
