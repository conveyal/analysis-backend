package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.conveyal.r5.analyst.BootstrapPercentileMethodHypothesisTestGridReducer;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.scenario.Scenario;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.SelectingGridReducer;
import com.conveyal.taui.analysis.broker.Broker;
import com.conveyal.taui.grids.GridExporter;
import com.conveyal.taui.models.AnalysisRequest;
import com.conveyal.taui.models.OpportunityDataset;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.mongodb.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Spark HTTP handler methods that allow launching new regional analyses, as well as deleting them and fetching
 * information about them.
 *
 * TODO make this non-static, constructing it by passing in a reference to the Broker instance.
 */
public class RegionalAnalysisController {

    private static final Logger LOG = LoggerFactory.getLogger(RegionalAnalysisController.class);
    public static final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withRegion(AnalysisServerConfig.awsRegion)
            .build();
    private static String BUCKET = AnalysisServerConfig.resultsBucket;

    // FIXME hackish - all other components can use the broker via this public field.
    public static final Broker broker = new Broker();

    public static Collection<RegionalAnalysis> getRegionalAnalysis (Request req, Response res) {
        return Persistence.regionalAnalyses.findPermitted(
                QueryBuilder.start().and(
                        QueryBuilder.start("regionId").is(req.params("regionId")).get(),
                        QueryBuilder.start("deleted").is(false).get()
                ).get(),
                req.attribute("accessGroup")
        );
    }

    public static RegionalAnalysis deleteRegionalAnalysis (Request req, Response res) {
        String accessGroup = req.attribute("accessGroup");
        String email = req.attribute("email");

        RegionalAnalysis analysis = Persistence.regionalAnalyses.findByIdFromRequestIfPermitted(req);
        analysis.deleted = true;
        Persistence.regionalAnalyses.updateByUserIfPermitted(analysis, email, accessGroup);

        // clear it from the broker
        if (!analysis.complete) {
            String jobId = analysis._id;
            if (broker.deleteJob(jobId)) {
                LOG.info("Deleted job {} from broker.", jobId);
            } else {
                LOG.error("Deleting job {} from broker failed.", jobId);
            }
        }
        return analysis;
    }

    /**
     * This used to extract a particular percentile of a regional analysis as a grid file.
     * Now it just gets the single percentile that exists for any one analysis, either from the local buffer file
     * for an analysis still in progress, or from S3 for a completed analysis.
     */
    public static Object getRegionalResults (Request req, Response res) throws IOException {

        // Get some path parameters out of the URL.
        // The UUID of the regional analysis for which we want the output data
        String regionalAnalysisId = req.params("_id");
        // The response file format: PNG, TIFF, or GRID
        final String formatString = req.params("format");

        RegionalAnalysis analysis = Persistence.regionalAnalyses.findByIdFromRequestIfPermitted(req);
        if (analysis == null || analysis.deleted) {
            throw AnalysisServerException.notFound("The specified regional analysis in unknown or has been deleted.");
        }

        // It seems like you would check regionalAnalysis.complete to choose between redirecting to s3 and fetching
        // the partially completed local file. But this field is never set to true - it's on a UI model object that
        // isn't readily accessible to the internal Job-tracking mechanism of the back end. Instead, just try to fetch
        // the partially completed results file, which includes an O(1) check whether the job is still being processed.
        File partialRegionalAnalysisResultFile = broker.getPartialRegionalAnalysisResults(regionalAnalysisId);

        if (partialRegionalAnalysisResultFile != null) {
            // The job is still being processed. There is a probably harmless race condition if the job happens to be
            // completed at the very moment we're in this block, because the file will be deleted at that moment.
            LOG.info("Analysis {} is not complete, attempting to return the partial results grid.", regionalAnalysisId);
            if (!"GRID".equalsIgnoreCase(formatString)) {
                throw AnalysisServerException.badRequest(
                        "For partially completed regional analyses, we can only return grid files, not images.");
            }
            if (partialRegionalAnalysisResultFile == null) {
                throw AnalysisServerException.unknown(
                        "Could not find partial result grid for incomplete regional analysis on server.");
            }
            try {
                res.header("content-type", "application/octet-stream");
                // This will cause Spark Framework to gzip the data automatically if requested by the client.
                res.header("Content-Encoding", "gzip");
                // Spark has default serializers for InputStream and Bytes, and calls toString() on everything else.
                return new FileInputStream(partialRegionalAnalysisResultFile);
            } catch (FileNotFoundException e) {
                // The job must have finished and the file was deleted upon upload to S3. This should be very rare.
                throw AnalysisServerException.unknown(
                        "Could not find partial result grid for incomplete regional analysis on server.");
            }
        } else {
            // The analysis has already completed, results should be stored and retrieved from S3 via redirects.
            GridExporter.Format format = GridExporter.format(formatString);
            String redirectText = req.queryParams("redirect");
            boolean redirect = GridExporter.checkRedirectAndFormat(redirectText, format);
            // Accessibility given X percentile travel time.
            // No need to record what the percentile is, that is currently fixed by the regional analysis.
            final String percentileGridKey = String.format("%s_given_percentile_travel_time.%s", regionalAnalysisId, formatString);
            String accessGridKey = String.format("%s.access", regionalAnalysisId);
            if (!s3.doesObjectExist(BUCKET, percentileGridKey)) {
                // The grid has not been built yet, make it.
                long computeStart = System.currentTimeMillis();
                // This is accessibility given x percentile travel time, the first sample is the point estimate
                // computed using all monte carlo draws, and subsequent samples are bootstrap replications. Return the
                // point estimate in the grids.
                LOG.info("Point estimate for regional analysis {} not found, building it", regionalAnalysisId);
                Grid grid = new SelectingGridReducer(0).compute(BUCKET, accessGridKey);
                LOG.info("Building grid took {}s", (System.currentTimeMillis() - computeStart) / 1000d);
                GridExporter.writeToS3(grid, s3, BUCKET, percentileGridKey, format);
            }
            return GridExporter.downloadFromS3(s3, BUCKET, percentileGridKey, redirect, res);
        }
    }

    /**
     * Get a probability of improvement between two regional analyses.
     * TODO remove this. But the UI is still calling it.
     */
    public static Object getProbabilitySurface (Request req, Response res) throws IOException {
        String regionalAnalysisId = req.params("_id");
        String comparisonId = req.params("comparisonId");
        String probabilitySurfaceName = String.format("%s_%s_probability", regionalAnalysisId, comparisonId);
        final String formatString = req.params("format");
        GridExporter.Format format = GridExporter.format(req.params("format"));
        String redirectText = req.queryParams("redirect");

        boolean redirect = GridExporter.checkRedirectAndFormat(redirectText, format);

        String probabilitySurfaceKey = String.format("%s.%s", probabilitySurfaceName, formatString);

        if (!s3.doesObjectExist(BUCKET, probabilitySurfaceKey)) {
            LOG.info("Probability surface for {} -> {} not found, building it", regionalAnalysisId, comparisonId);

            String regionalAccessKey = String.format("%s.access", regionalAnalysisId);
            String comparisonAccessKey = String.format("%s.access", comparisonId);

            // if these are bootstrapped travel times with a particular travel time percentile, use the bootstrap
            // p-value/hypothesis test computer. Otherwise use the older setup.
            // TODO should all comparisons use the bootstrap computer? the only real difference is that it is two-tailed.
            BootstrapPercentileMethodHypothesisTestGridReducer computer = new BootstrapPercentileMethodHypothesisTestGridReducer();

            Grid grid = computer.computeImprovementProbability(BUCKET, comparisonAccessKey, regionalAccessKey);
            GridExporter.writeToS3(grid, s3, BUCKET, probabilitySurfaceKey, format);
        }

        return GridExporter.downloadFromS3(s3, BUCKET, probabilitySurfaceKey, redirect, res);
    }

    /**
     * Deserialize a description of a new regional analysis (an AnalysisRequest object) POSTed as JSON over the HTTP API.
     * Derive an internal RegionalAnalysis object, which is enqueued in the broker and also returned to the caller
     * in the body of the HTTP response.
     */
    public static RegionalAnalysis createRegionalAnalysis (Request req, Response res) throws IOException {
        final String accessGroup = req.attribute("accessGroup");
        final String email = req.attribute("email");

        AnalysisRequest analysisRequest = JsonUtil.objectMapper.readValue(req.body(), AnalysisRequest.class);
        // If the UI has requested creation of a "static site", set all the necessary switches on the requests
        // that will go to the worker: break travel time down into waiting, riding, and walking, record paths to
        // destinations, and save results on S3.
        if (analysisRequest.name.contains("STATIC_SITE")) {
            // Hidden feature: allows us to run static sites experimentally without exposing a checkbox to all users.
            analysisRequest.makeStaticSite = true;
        }

        // Create an internal RegionalTask and RegionalAnalysis from the AnalysisRequest sent by the client.
        Project project = Persistence.projects.findByIdIfPermitted(analysisRequest.projectId, accessGroup);
        RegionalTask task = (RegionalTask) analysisRequest.populateTask(new RegionalTask(), project);

        // Set the destination grid.
        OpportunityDataset opportunityDataset = Persistence.opportunityDatasets.findByIdIfPermitted(analysisRequest.opportunityDatasetId, accessGroup);
        task.grid = opportunityDataset.getKey(GridExporter.Format.GRID);

        // Why are these being set to zero instead of leaving them at their default of -1?
        // Why does a regional analysis have an x and y at all since it represents many different tasks?
        task.x = 0;
        task.y = 0;

        // Making a static site implies several different processes - turn them all on if requested.
        if (analysisRequest.makeStaticSite) {
            task.makeStaticSite = true;
            task.travelTimeBreakdown = true;
            task.returnPaths = true;
        }

        // TODO remove duplicate fields from RegionalAnalysis that are already in the nested task.
        // The RegionalAnalysis should just be a minimal wrapper around the template task, adding the origin point set.
        // The RegionalAnalysis object contains a reference to the template task itself.
        // In fact, there are three separate classes all containing almost the same info:
        // AnalysisRequest, RegionalTask, RegionalAnalysis.
        RegionalAnalysis regionalAnalysis = new RegionalAnalysis();

        regionalAnalysis.request = task;

        regionalAnalysis.height = task.height;
        regionalAnalysis.north = task.north;
        regionalAnalysis.west = task.west;
        regionalAnalysis.width = task.width;

        regionalAnalysis.accessGroup = accessGroup;
        regionalAnalysis.bundleId = project.bundleId;
        regionalAnalysis.createdBy = email;
        regionalAnalysis.cutoffMinutes = task.maxTripDurationMinutes;
        regionalAnalysis.grid = analysisRequest.opportunityDatasetId;
        regionalAnalysis.name = analysisRequest.name;
        regionalAnalysis.projectId = analysisRequest.projectId;
        regionalAnalysis.regionId = project.regionId;
        regionalAnalysis.travelTimePercentile = analysisRequest.travelTimePercentile;
        regionalAnalysis.variant = analysisRequest.variantIndex;
        regionalAnalysis.workerVersion = analysisRequest.workerVersion;
        regionalAnalysis.zoom = task.zoom;

        // Persist this newly created RegionalAnalysis to Mongo.
        // Why are we overwriting regionalAnalysis with the result of saving it?
        regionalAnalysis = Persistence.regionalAnalyses.create(regionalAnalysis);

        // The single RegionalAnalysis object represents a lot of individual accessibility tasks at many different
        // origin points, typically on a grid. Before passing that object on to the Broker (which distributes tasks to
        // workers and tracks progress), we remove the details of the scenario, substituting the scenario's unique ID
        // to save time and bandwidth. This avoids repeatedly sending the scenario details to the worker in every task,
        // as they are often quite voluminous. The workers will fetch the scenario once from S3 and cache it based on
        // its ID only. We protectively clone this task because we're going to null out its scenario field, and don't
        // want to affect the original object which contains all the scenario details.
        RegionalTask templateTask = regionalAnalysis.request.clone();
        Scenario scenario = templateTask.scenario;
        templateTask.scenarioId = scenario.id;
        templateTask.scenario = null;
        String fileName = String.format("%s_%s.json", regionalAnalysis.bundleId, scenario.id);
        File cachedScenario = new File(AnalysisServerConfig.localCacheDirectory, fileName);
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

        // Fill in all the fields in the template task that will remain the same across all tasks in a job.
        // Re-setting all these fields may not be necessary (they might already be set previously),
        // but we can't eliminate these lines without thoroughly checking that assumption.
        templateTask.jobId = regionalAnalysis._id;
        templateTask.graphId = regionalAnalysis.bundleId;
        templateTask.workerVersion = regionalAnalysis.workerVersion;
        templateTask.height = regionalAnalysis.height;
        templateTask.width = regionalAnalysis.width;
        templateTask.north = regionalAnalysis.north;
        templateTask.west = regionalAnalysis.west;
        templateTask.zoom = regionalAnalysis.zoom;
        templateTask.maxTripDurationMinutes = regionalAnalysis.cutoffMinutes;
        templateTask.percentiles = new double[] { regionalAnalysis.travelTimePercentile };
        templateTask.grid = opportunityDataset.getKey(GridExporter.Format.GRID);

        // Register the regional job with the broker, which will distribute individual tasks to workers and track progress.
        broker.enqueueTasksForRegionalJob(templateTask, regionalAnalysis.accessGroup, regionalAnalysis.createdBy);

        return regionalAnalysis;
    }

    public static RegionalAnalysis updateRegionalAnalysis(Request request, Response response) throws IOException {
        final String accessGroup = request.attribute("accessGroup");
        final String email = request.attribute("email");
        RegionalAnalysis regionalAnalysis = JsonUtil.objectMapper.readValue(request.body(), RegionalAnalysis.class);
        return Persistence.regionalAnalyses.updateByUserIfPermitted(regionalAnalysis, email, accessGroup);
    }

    public static void register () {
        get("/api/region/:regionId/regional", RegionalAnalysisController::getRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        // For grids, no transformer is supplied: render raw bytes or input stream rather than transforming to JSON.
        get("/api/regional/:_id/grid/:format", RegionalAnalysisController::getRegionalResults);
        get("/api/regional/:_id/:comparisonId/:format", RegionalAnalysisController::getProbabilitySurface, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:_id/:comparisonId/:format", RegionalAnalysisController::getProbabilitySurface, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/regional/:_id", RegionalAnalysisController::deleteRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        post("/api/regional", RegionalAnalysisController::createRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        put("/api/regional/:_id", RegionalAnalysisController::updateRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
    }

}
