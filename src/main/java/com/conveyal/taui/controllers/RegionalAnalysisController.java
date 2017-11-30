package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.conveyal.r5.analyst.BootstrapPercentileMethodHypothesisTestGridReducer;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.SelectingGridReducer;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.analysis.RegionalAnalysisManager;
import com.conveyal.taui.grids.GridExporter;
import com.conveyal.taui.models.AnalysisRequest;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.persistence.TiledAccessGrid;
import com.conveyal.taui.util.JsonUtil;
import com.mongodb.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Created by matthewc on 10/21/16.
 */
public class RegionalAnalysisController {
    private static final Logger LOG = LoggerFactory.getLogger(RegionalAnalysisController.class);
    private static AmazonS3 s3 = new AmazonS3Client();
    private static String BUCKET = AnalysisServerConfig.resultsBucket;

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
            RegionalAnalysisManager.deleteJob(analysis._id);
        }

        return analysis;
    }

    /** Get a particular percentile of a query as a grid file */
    public static Object getPercentile (Request req, Response res) throws IOException {
        String regionalAnalysisId = req.params("_id");
        RegionalAnalysis analysis = Persistence.regionalAnalyses.findByIdFromRequestIfPermitted(req);

        // while we can do non-integer percentiles, don't allow that here to prevent cache misses
        String format = req.params("format").toLowerCase();
        String redirectText = req.queryParams("redirect");

        boolean redirect = GridExporter.checkRedirectAndFormat(redirectText, format);
        String percentileGridKey;

        if (analysis.travelTimePercentile == -1) {
            // Andrew Owen style average instantaneous accessibility
            percentileGridKey = String.format("%s_average.%s", regionalAnalysisId, format);
        } else {
            // accessibility given X percentile travel time
            // use the point estimate when there are many bootstrap replications of the accessibility given median
            // accessibility
            // no need to record what the percentile is, that is fixed by the regional analysis.
            percentileGridKey = String.format("%s_given_percentile_travel_time.%s", regionalAnalysisId, format);
        }

        String accessGridKey = String.format("%s.access", regionalAnalysisId);

        if (!s3.doesObjectExist(BUCKET, percentileGridKey)) {
            // make the grid
            Grid grid;
            long computeStart = System.currentTimeMillis();
            if (analysis.travelTimePercentile == -1) {
                // Andrew Owen style average instantaneous accessibility
                // The samples stored in the access grid are samples of instantaneous accessibility at different minutes
                // and Monte Carlo draws, average them together
                throw new IllegalArgumentException("Old-style instantaneous-accessibility regional analyses are no longer supported");
            } else {
                // This is accessibility given x percentile travel time, the first sample is the point estimate
                // computed using all monte carlo draws, and subsequent samples are bootstrap replications. Return the
                // point estimate in the grids.
                LOG.info("Point estimate for regional analysis {} not found, building it", regionalAnalysisId);
                grid = new SelectingGridReducer(0).compute(BUCKET, accessGridKey);
            }
            LOG.info("Building grid took {}s", (System.currentTimeMillis() - computeStart) / 1000d);

            GridExporter.writeToS3(grid, s3, BUCKET, String.format("%s_given_percentile_travel_time", regionalAnalysisId), format);
        }

        return GridExporter.downloadFromS3(s3, BUCKET, percentileGridKey, redirect, res);

    }

    /** Get a probability of improvement from a baseline to a project */
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

            String baseKey = String.format("%s.access", regionalAnalysisId);
            String scenarioKey = String.format("%s.access", comparisonId);

            // if these are bootstrapped travel times with a particular travel time percentile, use the bootstrap
            // p-value/hypothesis test computer. Otherwise use the older setup.
            // TODO should all comparisons use the bootstrap computer? the only real difference is that it is two-tailed.
            BootstrapPercentileMethodHypothesisTestGridReducer computer = new BootstrapPercentileMethodHypothesisTestGridReducer();

            Grid grid = computer.computeImprovementProbability(BUCKET, baseKey, scenarioKey);
            GridExporter.writeToS3(grid, s3, BUCKET, probabilitySurfaceName, format);
        }

        return GridExporter.downloadFromS3(s3, BUCKET, probabilitySurfaceKey, redirect, res);
    }

    public static int[] getSamplingDistribution (Request req, Response res) {
        String regionalAnalysisId = req.params("_id");
        double lat = Double.parseDouble(req.params("lat"));
        double lon = Double.parseDouble(req.params("lon"));

        return TiledAccessGrid
                .get(BUCKET,  String.format("%s.access", regionalAnalysisId))
                .getLatLon(lat, lon);
    }

    public static RegionalAnalysis createRegionalAnalysis (Request req, Response res) throws IOException {
        final String accessGroup = req.attribute("accessGroup");
        final String email = req.attribute("email");

        AnalysisRequest analysisRequest = JsonUtil.objectMapper.readValue(req.body(), AnalysisRequest.class);
        Project project = Persistence.projects.findByIdIfPermitted(analysisRequest.projectId, accessGroup);
        RegionalTask task = (RegionalTask) analysisRequest.populateTask(new RegionalTask(), project);

        task.grid = String.format("%s/%s.grid", project.regionId, analysisRequest.opportunityDatasetKey);
        task.x = 0;
        task.y = 0;

        // TODO remove duplicate data that is already in the task
        RegionalAnalysis regionalAnalysis = new RegionalAnalysis();

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
        regionalAnalysis.request = task;
        regionalAnalysis.travelTimePercentile = analysisRequest.travelTimePercentile;
        regionalAnalysis.variant = analysisRequest.variantIndex;
        regionalAnalysis.workerVersion = analysisRequest.workerVersion;
        regionalAnalysis.zoom = task.zoom;

        regionalAnalysis = Persistence.regionalAnalyses.create(regionalAnalysis);
        RegionalAnalysisManager.enqueue(regionalAnalysis);

        return regionalAnalysis;
    }

    public static void register () {
        get("/api/region/:regionId/regional", RegionalAnalysisController::getRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:_id/grid/:format", RegionalAnalysisController::getPercentile, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:_id/samplingDistribution/:lat/:lon", RegionalAnalysisController::getSamplingDistribution, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:_id/:comparisonId/:format", RegionalAnalysisController::getProbabilitySurface, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/regional/:_id", RegionalAnalysisController::deleteRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        post("/api/regional", RegionalAnalysisController::createRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
    }

}
