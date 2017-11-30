package com.conveyal.taui.analysis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.broker.JobStatus;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
import com.conveyal.r5.analyst.cluster.GridResultAssembler;
import com.conveyal.r5.analyst.cluster.GridResultQueueConsumer;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.scenario.Scenario;
import com.conveyal.r5.profile.ProfileRequest;
import com.conveyal.taui.models.Region;
import com.conveyal.taui.persistence.TiledAccessGrid;
import com.conveyal.taui.util.HttpUtil;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.persistence.Persistence;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Manages coordination of multipoint runs with the broker.
 */
public class RegionalAnalysisManager {
    private static final Logger LOG = LoggerFactory.getLogger(RegionalAnalysisManager.class);
    private static AmazonS3 s3 = new AmazonS3Client();

    private static ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 20, 90, TimeUnit.SECONDS, new ArrayBlockingQueue<>(512));

    public static Map<String, JobStatus> statusByJob;

    public static final GridResultQueueConsumer consumer;
    public static final String resultsQueueUrl;
    private static final int REQUEST_CHUNK_SIZE = 1000;

    public static final String brokerUrl = AnalysisServerConfig.brokerUrl;

    static {
        AmazonSQS sqs = new AmazonSQSClient();
        sqs.setRegion(com.amazonaws.regions.Region.getRegion(Regions.fromName(AnalysisServerConfig.region)));
        resultsQueueUrl = sqs.getQueueUrl(AnalysisServerConfig.resultsQueue).getQueueUrl();
        consumer = new GridResultQueueConsumer(resultsQueueUrl, AnalysisServerConfig.resultsBucket);

        new Thread(consumer, "queue-consumer").start();
    }

    public static void enqueue (RegionalAnalysis regionalAnalysis) {
        executor.execute(() -> {
            // Replace the scenario with only its ID, and save the scenario to be fetched by the workers.
            // This avoids having 2 million identical copies of the same scenario going over the wire, and being
            // saved in memory on the broker.
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

            Bundle bundle = Persistence.bundles.get(regionalAnalysis.bundleId);
            Region region = Persistence.regions.get(bundle.regionId);

            // Fill in all the fields that will remain the same across all tasks in a job.
            // Re-setting all these fields may not be necessary (they might already be set by the caller),
            // but we can't eliminate these lines without thoroughly checking that assumption.
            templateTask.jobId = regionalAnalysis.id;
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
            templateTask.grid = String.format("%s/%s.grid", project.id, regionalAnalysis.grid);

            try {
                LOG.info("Enqueuing tasks for job {} using template task.", templateTask.jobId);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                JsonUtil.objectMapper.writeValue(baos, templateTask);

                HttpPost post = new HttpPost(String.format("%s/enqueue/regional", brokerUrl));
                post.setEntity(new ByteArrayEntity(baos.toByteArray()));
                CloseableHttpResponse res = null;

                try {
                    res = HttpUtil.httpClient.execute(post);
                    LOG.info("Enqueued job {} to broker. Response status: {}", templateTask.jobId, res.getStatusLine().getStatusCode());
                    EntityUtils.consume(res.getEntity());
                } finally {
                    if (res != null) res.close();
                }
            } catch (IOException e) {
                LOG.error("error enqueueing requests", e);
                throw new RuntimeException("error enqueueing requests", e);
            }

            consumer.registerJob(templateTask,
                    new TilingGridResultAssembler(templateTask, AnalysisServerConfig.resultsBucket));

        });
    }

    public static void deleteJob(String jobId) {
        CloseableHttpResponse res = null;
        try {
            HttpDelete del = new HttpDelete(String.format("%s/jobs/%s", brokerUrl, jobId));
            res = HttpUtil.httpClient.execute(del);
            EntityUtils.consume(res.getEntity());
        } catch (IOException e) {
            LOG.error("error deleting job {}", e);
        } finally {
            if (res != null) try {
                res.close();
            } catch (IOException e) {
                LOG.error("error deleting job {}", e);
            }
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
