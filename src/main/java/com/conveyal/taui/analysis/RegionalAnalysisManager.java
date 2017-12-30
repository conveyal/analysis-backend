package com.conveyal.taui.analysis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.conveyal.r5.analyst.broker.Broker;
import com.conveyal.r5.analyst.broker.JobStatus;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
import com.conveyal.r5.analyst.cluster.GridResultAssembler;
import com.conveyal.r5.analyst.cluster.GridResultQueueConsumer;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.scenario.Scenario;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.persistence.TiledAccessGrid;
import com.conveyal.taui.util.HttpUtil;
import com.conveyal.taui.util.JsonUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Manages coordination of multipoint runs with the broker.
 * FIXME this whole thing is static, it should be an instance.
 */
public class RegionalAnalysisManager {

    private static final Logger LOG = LoggerFactory.getLogger(RegionalAnalysisManager.class);
    private static AmazonS3 s3 = new AmazonS3Client();
    public static Broker broker;
    public static final GridResultQueueConsumer consumer;
    public static final String resultsQueueUrl;

    static {
        AmazonSQS sqs = new AmazonSQSClient();
        sqs.setRegion(com.amazonaws.regions.Region.getRegion(Regions.fromName(AnalysisServerConfig.region)));
        resultsQueueUrl = sqs.getQueueUrl(AnalysisServerConfig.resultsQueue).getQueueUrl();
        consumer = new GridResultQueueConsumer();
        // Here we used to create and start a new thread for the queue consumer. Now the results are stored
        // synchronously on the backend in the HTTP handler used by workers to return their results.
    }

    public static void enqueue (RegionalAnalysis regionalAnalysis) {
        // TODO have the backend supply the scenarios over an HTTP API to the workers (which would then cache them), so we don't need to use S3?
        RegionalTask templateTask = regionalAnalysis.request.clone();
        Scenario scenario = templateTask.scenario;
        templateTask.scenarioId = scenario.id;
        templateTask.scenario = null;

        String fileName = String.format("%s_%s.json", regionalAnalysis.bundleId, scenario.id);
        File cachedScenario = new File(AnalysisServerConfig.localCache, fileName);
        try {
            JsonUtil.objectMapper.writeValue(cachedScenario, scenario);
        } catch (IOException e) {
            LOG.error("Error saving scenario to disk", e);
        }

        if (!AnalysisServerConfig.offline) {
            // Upload the scenario to S3 where workers can fetch it by ID.
            s3.putObject(AnalysisServerConfig.bundleBucket, fileName, cachedScenario);
        }

        // Fill in all the fields that will remain the same across all tasks in a job.
        // Re-setting all these fields may not be necessary (they might already be set by the caller),
        // but we can't eliminate these lines without thoroughly checking that assumption.
        templateTask.jobId = regionalAnalysis._id;
        templateTask.graphId = regionalAnalysis.bundleId;
        templateTask.workerVersion = regionalAnalysis.workerVersion;
        templateTask.height = regionalAnalysis.height;
        templateTask.width = regionalAnalysis.width;
        templateTask.north = regionalAnalysis.north;
        templateTask.west = regionalAnalysis.west;
        templateTask.zoom = regionalAnalysis.zoom;
        templateTask.outputQueue = resultsQueueUrl;
        templateTask.maxTripDurationMinutes = regionalAnalysis.cutoffMinutes;
        templateTask.percentiles = new double[] { regionalAnalysis.travelTimePercentile };
        templateTask.grid = String.format("%s/%s.grid", regionalAnalysis.regionId, regionalAnalysis.grid);

        broker.enqueueTasksForRegionalJob(templateTask);

        // FIXME why is this a "tiling" grid result assembler?
        consumer.registerJob(templateTask,
                new TilingGridResultAssembler(templateTask, AnalysisServerConfig.resultsBucket));
    }

    public static void deleteJob(String jobId) {
        if (broker.deleteJob(jobId)) {
            LOG.info("Deleted job {} from broker.", jobId);
        } else {
            LOG.error("Deleting job {} from broker failed.", jobId);
        }
        // free temp disk space
        consumer.deleteJob(jobId);
    }

    public static RegionalAnalysisStatus getStatus (String jobId) {
        return consumer.assemblers.containsKey(jobId) ? new RegionalAnalysisStatus(consumer.assemblers.get(jobId)) : null;
    }

    public static final class RegionalAnalysisStatus implements Serializable {
        public int total;
        public int complete;

        public RegionalAnalysisStatus () { /* do nothing */ }

        public RegionalAnalysisStatus (GridResultAssembler assembler) {
            total = assembler.nTotal;
            complete = assembler.nComplete;
        }
    }

    /** A GridResultAssembler that tiles the results once they are complete */
    public static class TilingGridResultAssembler extends GridResultAssembler {
        public TilingGridResultAssembler(AnalysisTask request, String outputBucket) {
            super(request, outputBucket);
        }

        @Override
        protected synchronized void finish () {
            super.finish();
            // build the tiles (used to display sampling distributions in the client)
            // Note that the job will be marked as complete even before the tiles are built, but this is okay;
            // the tiles are not needed to display the regional analysis, only to display sampling distributions from it
            // the user can view the results immediately, and the sampling distribution loading will block until the tiles
            // are built thanks to the use of a Guava loadingCache below (which will only build the value for a particular key
            // once)
            TiledAccessGrid.get(outputBucket, String.format("%s.access", request.jobId));
        }
    }
}
