package com.conveyal.taui.analysis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.conveyal.r5.analyst.cluster.RegionalWorkResult;
import com.conveyal.taui.analysis.broker.Broker;
import com.conveyal.r5.analyst.cluster.GridResultAssembler;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.scenario.Scenario;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages coordination of multipoint runs with the broker.
 * FIXME this whole thing is static, it should be an instance.
 */
public class RegionalAnalysisManager {

    private static final Logger LOG = LoggerFactory.getLogger(RegionalAnalysisManager.class);
    private static AmazonS3 s3 = new AmazonS3Client();
    public static Broker broker;
    public static final String resultsQueueUrl;

    // FIXME use less static fields
    private static Map<String, GridResultAssembler> resultAssemblers = new HashMap<>();

    static {
        AmazonSQS sqs = new AmazonSQSClient();
        sqs.setRegion(com.amazonaws.regions.Region.getRegion(Regions.fromName(AnalysisServerConfig.region)));
        resultsQueueUrl = sqs.getQueueUrl(AnalysisServerConfig.resultsQueue).getQueueUrl();
    }

    /**
     * This function is called with a single RegionalAnalysis object that actually represents a lot of individual
     * accessibility tasks at many different origin points, typically on a grid.
     *
     * Before passing that object on to the Broker (which distributes tasks to workers and tracks progress), this method
     * removes the details of the transportation scenario, replacing it with a unique ID. This avoids repeatedly sending
     * the scenario details to the worker in every task, as they are often quite voluminous.
     */
    public static void enqueue (RegionalAnalysis regionalAnalysis) {

        // First, replace the details of the scenario with only its unique ID to save time and bandwidth.
        // The workers can fetch the scenario once and cache it based on its ID only.
        // We protectively clone this task because we're going to null out its scenario field, and don't want to affect
        // the reference held by the caller, which contains all the scenario details.
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
            // TODO have the backend supply the scenarios over an HTTP API to the workers (which would then cache them), so we don't need to use S3?
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

        // Register the regional job with the broker, which will distribute individual tasks to workers and track progress.
        broker.enqueueTasksForRegionalJob(templateTask);

        // Register the regional job so results received from multiple workers can be assembled into one file.
        // TODO possibly just merge this result assembly functionality into the broker
        resultAssemblers.put(templateTask.jobId, new GridResultAssembler(templateTask, AnalysisServerConfig.resultsBucket));
    }

    public static void deleteJob(String jobId) {
        // First, remove the job from the broker so we stop distributing its tasks to workers.
        if (broker.deleteJob(jobId)) {
            LOG.info("Deleted job {} from broker.", jobId);
        } else {
            LOG.error("Deleting job {} from broker failed.", jobId);
        }
        // Then shut down the object used for assembling results, removing its associated temporary disk file.
        GridResultAssembler assembler = resultAssemblers.remove(jobId);
        try {
            assembler.terminate();
        } catch (Exception e) {
            LOG.error("Could not terminate grid result assembler, this may waste disk space. Reason: {}", e.toString());
        }
        // TODO where do we delete the regional analysis from Persistence so it doesn't show up in the UI after deletion?
    }

    public static RegionalAnalysisStatus getStatus (String jobId) {
        return resultAssemblers.containsKey(jobId) ? new RegionalAnalysisStatus(resultAssemblers.get(jobId)) : null;
    }

    public static void handleRegionalWorkResult(RegionalWorkResult workResult) {
        GridResultAssembler assembler = resultAssemblers.get(workResult.jobId);
        if (assembler == null) {
            LOG.error("Received result for unrecognized job ID {}, discarding.", workResult.jobId);
        } else {
            assembler.handleMessage(workResult);
        }
    }

    public static final class RegionalAnalysisStatus implements Serializable {
        public int total;
        public int complete;

        public RegionalAnalysisStatus () { /* No-arg constructor for deserialization only. */ }

        public RegionalAnalysisStatus (GridResultAssembler assembler) {
            total = assembler.nTotal;
            complete = assembler.nComplete;
        }
    }

}
