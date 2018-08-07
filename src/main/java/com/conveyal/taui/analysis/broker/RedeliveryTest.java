package com.conveyal.taui.analysis.broker;

import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.taui.AnalysisServer;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.controllers.RegionalAnalysisController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * This test is not an automatic unit test. It is an integration test that must be started manually, because it takes
 * a long time to run. It will start up local analysis server with local workers, then submit a  job. The workers
 * are configured to fail to complete tasks some percentage of the time, but eventually the whole job should be finished
 * because the broker will redeliver lost tasks to the workers.
 * Progress can be followed by watching the admin page of the local server at...
 */
public class RedeliveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(RedeliveryTest.class);

    static final int N_JOBS = 4;
    static final int N_TASKS_PER_JOB = 100;

    /**
     * @param params are not used.
     */
    public static void main(String[] params) {

        // Start an analysis server with the default (offline) properties.
        AnalysisServerConfig.testTaskRedelivery = true;
        AnalysisServer.main();

        // Feed several jobs to the broker, staggered in time, to ensure redelivery can deal with multiple jobs.
        for (int j = 0; j < N_JOBS; j++) {
            sendFakeJob();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) { }
        }

        // Wait for all tasks to be marked finished.
        while (RegionalAnalysisController.broker.anyJobsActive()) {
            RegionalAnalysisController.broker.logJobStatus();
            try {
                LOG.info("Some jobs are still not complete.");
                Thread.sleep(2000);
            } catch (InterruptedException e) { }
        }

        LOG.info("All jobs finished.");
        System.exit(0);
    }

    private static void sendFakeJob() {
        RegionalTask templateTask = new RegionalTask();
        templateTask.jobId = compactUUID();
        templateTask.west = 0;
        templateTask.north = 0;
        templateTask.height = 1;
        templateTask.width = N_TASKS_PER_JOB;
        templateTask.scenarioId = "FAKE";
        RegionalAnalysisController.broker.enqueueTasksForRegionalJob(templateTask,"test", "test");
    }

    public static String compactUUID() {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = new byte[16];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        String hex = uuid.toString().replaceAll("-", "");
        return hex;
    }

}
