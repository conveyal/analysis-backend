package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.conveyal.r5.analyst.BootstrapPercentileMethodHypothesisTestGridReducer;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.SelectingGridReducer;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.scenario.Scenario;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.analysis.broker.Broker;
import com.conveyal.taui.grids.GridExporter;
import com.conveyal.taui.models.AnalysisRequest;
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
import java.io.IOException;
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
    private static final String awsRegion = AnalysisServerConfig.awsRegion;
    private static final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withRegion(awsRegion)
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
     * Get a particular percentile of a query as a grid file.
     * FIXME this does not seem to do that anymore. It just gets the single percentile that exists for any one analysis.
     */
    public static Object getPercentile (Request req, Response res) throws IOException {
        String regionalAnalysisId = req.params("_id");
        Persistence.regionalAnalyses.findByIdFromRequestIfPermitted(req);
        // The response file format: PNG, TIFF, or GRID
        String format = req.params("format").toLowerCase();
        String redirectText = req.queryParams("redirect");
        boolean redirect = GridExporter.checkRedirectAndFormat(redirectText, format);

        // Accessibility given X percentile travel time.
        // No need to record what the percentile is, that is currently fixed by the regional analysis.
        final String percentileGridKey = String.format("%s_given_percentile_travel_time.%s", regionalAnalysisId, format);
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

    /**
     * Get a probability of improvement between two regional analyses.
     * TODO remove this. But the UI is still calling it.
     */
    public static Object getProbabilitySurface (Request req, Response res) throws IOException {
        String regionalAnalysisId = req.params("_id");
        String comparisonId = req.params("comparisonId");
        String probabilitySurfaceName = String.format("%s_%s_probability", regionalAnalysisId, comparisonId);
        String format = req.params("format").toLowerCase();
        String redirectText = req.queryParams("redirect");

        boolean redirect = GridExporter.checkRedirectAndFormat(redirectText, format);

        String probabilitySurfaceKey = String.format("%s.%s", probabilitySurfaceName, format);

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
        task.grid = String.format("%s/%s.grid", project.regionId, analysisRequest.opportunityDatasetKey);

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
        regionalAnalysis.grid = analysisRequest.opportunityDatasetKey;
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
        templateTask.grid = String.format("%s/%s.grid", regionalAnalysis.regionId, regionalAnalysis.grid);

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
        get("/api/regional/:_id/grid/:format", RegionalAnalysisController::getPercentile, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:_id/:comparisonId/:format", RegionalAnalysisController::getProbabilitySurface, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/regional/:_id", RegionalAnalysisController::deleteRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        post("/api/regional", RegionalAnalysisController::createRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        put("/api/regional/:_id", RegionalAnalysisController::updateRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
    }

}
