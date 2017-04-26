package com.conveyal.taui.controllers;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.r5.analyst.ExtractingGridStatisticComputer;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.ImprovementProbabilityGridStatisticComputer;
import com.conveyal.r5.analyst.PercentileGridStatisticComputer;
import com.conveyal.r5.analyst.scenario.AndrewOwenMeanGridStatisticComputer;
import com.conveyal.r5.util.S3Util;
import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.analysis.RegionalAnalysisManager;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.conveyal.taui.grids.SeamlessCensusGridExtractor.ZOOM;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static spark.Spark.get;
import static spark.Spark.delete;
import static spark.Spark.halt;
import static spark.Spark.post;

/**
 * Created by matthewc on 10/21/16.
 */
public class RegionalAnalysisController {
    private static final Logger LOG = LoggerFactory.getLogger(RegionalAnalysisController.class);

    public static AmazonS3 s3 = new AmazonS3Client();

    /** use a single thread executor so that the writing thread does not die before the S3 upload is finished */
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    /** How long request URLs are good for */
    public static final int REQUEST_TIMEOUT_MSEC = 15 * 1000;

    public static List<RegionalAnalysis> getRegionalAnalysis (Request req, Response res) {
        String projectId = req.params("projectId");
        return Persistence.regionalAnalyses.values().stream()
                .filter(q -> projectId.equals(q.projectId))
                .collect(Collectors.toList());
    }

    public static RegionalAnalysis deleteRegionalAnalysis (Request req, Response res) {
        // NB no need for security, UUID provides sufficient security
        RegionalAnalysis analysis = Persistence.regionalAnalyses.get(req.params("regionalAnalysisId"));
        analysis.clone();
        analysis.deleted = true;
        Persistence.regionalAnalyses.put(analysis.id, analysis);

        // clear it from the broker
        if (!analysis.complete) {
            RegionalAnalysisManager.deleteJob(analysis.id);
        }

        return analysis;
    }

    /** Get a particular percentile of a query as a grid file */
    public static Object getPercentile (Request req, Response res) throws IOException {
        String regionalAnalysisId = req.params("regionalAnalysisId");
        RegionalAnalysis analysis = Persistence.regionalAnalyses.get(regionalAnalysisId);

        if (analysis == null) halt(404);

        // while we can do non-integer percentiles, don't allow that here to prevent cache misses
        String format = req.params("format").toLowerCase();
        String percentileGridKey;

        String redirectText = req.queryParams("redirect");
        boolean redirect;
        if (redirectText == null || "" .equals(redirectText)) redirect = true;
        else redirect = parseBoolean(redirectText);

        if (!"grid".equals(format) && !"png".equals(format) && !"tiff".equals(format)) {
            halt(400);
        }


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

        if (!s3.doesObjectExist(AnalystConfig.resultsBucket, percentileGridKey)) {
            // make the grid
            Grid grid;
            long computeStart = System.currentTimeMillis();
            if (analysis.travelTimePercentile == -1) {
                // Andrew Owen style average instantaneous accessibility
                // The samples stored in the access grid are samples of instantaneous accessibility at different minutes
                // and Monte Carlo draws, average them together
                LOG.info("Mean for regional analysis {} not found, building it", regionalAnalysisId);
                grid = new AndrewOwenMeanGridStatisticComputer().compute(AnalystConfig.resultsBucket, accessGridKey);
            } else {
                // This is accessibility given x percentile travel time, the first sample is the point estimate
                // computed using all monte carlo draws, and subsequent samples are bootstrap replications. Return the
                // point estimate in the grids.
                LOG.info("Point estimate for regional analysis {} not found, building it", regionalAnalysisId);
                grid = new ExtractingGridStatisticComputer(0).compute(AnalystConfig.resultsBucket, accessGridKey);
            }
            LOG.info("Building grid took {}s", (computeStart - System.currentTimeMillis()) / 1000d);

            PipedInputStream pis = new PipedInputStream();
            PipedOutputStream pos = new PipedOutputStream(pis);

            ObjectMetadata om = new ObjectMetadata();

            if ("grid".equals(format)) {
                om.setContentType("application/octet-stream");
                om.setContentEncoding("gzip");
            } else if ("png".equals(format)) {
                om.setContentType("image/png");
            } else if ("tiff".equals(format)) {
                om.setContentType("image/tiff");
            }

            executorService.execute(() -> {
                try {
                    if ("grid".equals(format)) {
                        grid.write(new GZIPOutputStream(pos));
                    } else if ("png".equals("format")) {
                        grid.writePng(pos);
                    } else if ("tiff".equals(format)) {
                        grid.writeGeotiff(pos);
                    }
                } catch (IOException e) {
                    LOG.info("Error writing percentile to S3", e);
                }
            });

            // not using S3Util.streamToS3 because we need to make sure the put completes before we return
            // the URL, as the client will go to it immediately.
            s3.putObject(AnalystConfig.resultsBucket, percentileGridKey, pis, om);
        }

        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(AnalystConfig.resultsBucket, percentileGridKey);
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);
        URL url = s3.generatePresignedUrl(presigned);

        if (redirect) {
            res.type("text/plain"); // override application/json
            res.redirect(url.toString());
            res.status(302); // temporary redirect, this URL will soon expire
            return null;
        } else {
            return new WrappedURL(url.toString());
        }
    }

    /** Get a probability of improvement from a baseline to a scenario */
    public static Object getProbabilitySurface (Request req, Response res) throws IOException {
        String base = req.params("baseId");
        String scenario = req.params("scenarioId");
        String format = req.params("format").toLowerCase();

        String redirectText = req.queryParams("redirect");
        boolean redirect;
        if (redirectText == null || "" .equals(redirectText)) redirect = true;
        else redirect = parseBoolean(redirectText);


        if (!"grid".equals(format) && !"png".equals(format) && !"tiff".equals(format)) {
            halt(400);
        }

        String probabilitySurfaceKey = String.format("%s_%s_probability.%s", base, scenario, format);

        if (!s3.doesObjectExist(AnalystConfig.resultsBucket, probabilitySurfaceKey)) {
            LOG.info("Probability surface for {} -> {} not found, building it", base, scenario);

            String baseKey = String.format("%s.access", base);
            String scenarioKey = String.format("%s.access", scenario);

            Grid grid = ImprovementProbabilityGridStatisticComputer
                    .computeImprovementProbability(AnalystConfig.resultsBucket, baseKey, scenarioKey);

            PipedInputStream pis = new PipedInputStream();
            PipedOutputStream pos = new PipedOutputStream(pis);

            ObjectMetadata om = new ObjectMetadata();
            if ("grid".equals(format)) {
                om.setContentType("application/octet-stream");
                om.setContentEncoding("gzip");
            } else if ("png".equals(format)) {
                om.setContentType("image/png");
            } else if ("tiff".equals(format)) {
                om.setContentType("image/tiff");
            }

            executorService.execute(() -> {
                try {
                    if ("grid".equals(format)) {
                        grid.write(new GZIPOutputStream(pos));
                    } else if ("png".equals("format")) {
                        grid.writePng(pos);
                    } else if ("tiff".equals(format)) {
                        grid.writeGeotiff(pos);
                    }
                } catch (IOException e) {
                    LOG.info("Error writing probability surface to S3", e);
                }
            });

            // not using S3Util.streamToS3 because we need to make sure the put completes before we return
            // the URL, as the client will go to it immediately.
            s3.putObject(AnalystConfig.resultsBucket, probabilitySurfaceKey, pis, om);
        }

        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(AnalystConfig.resultsBucket, probabilitySurfaceKey);
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);
        URL url = s3.generatePresignedUrl(presigned);

        if (redirect) {
            res.type("text/plain"); // override default application/json
            res.redirect(url.toString());
            res.status(302); // temporary redirect, this URL will soon expire
            return null;
        } else {
            return new WrappedURL(url.toString());
        }
    }

    public static RegionalAnalysis createRegionalAnalysis (Request req, Response res) throws IOException {
        RegionalAnalysis regionalAnalysis = JsonUtil.objectMapper.readValue(req.body(), RegionalAnalysis.class);

        Bundle bundle = Persistence.bundles.get(regionalAnalysis.bundleId);
        Project project = Persistence.projects.get(bundle.projectId);

        // fill in the fields
        regionalAnalysis.id = UUID.randomUUID().toString();
        regionalAnalysis.projectId = project.id;

        // this scenario is specific to this job
        regionalAnalysis.request.scenarioId = null;
        regionalAnalysis.request.scenario.id = regionalAnalysis.id;

        regionalAnalysis.creationTime = System.currentTimeMillis();

        regionalAnalysis.zoom = 9;

        if (regionalAnalysis.bounds != null) regionalAnalysis.computeBoundingBoxFromBounds();
        else if (regionalAnalysis.width == 0) regionalAnalysis.computeBoundingBoxFromProject(project);

        Persistence.regionalAnalyses.put(regionalAnalysis.id, regionalAnalysis);
        RegionalAnalysisManager.enqueue(regionalAnalysis);

        return regionalAnalysis;
    }

    public static void register () {
        get("/api/project/:projectId/regional", RegionalAnalysisController::getRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:regionalAnalysisId/grid/:format", RegionalAnalysisController::getPercentile, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:baseId/:scenarioId/:format", RegionalAnalysisController::getProbabilitySurface, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/regional/:regionalAnalysisId", RegionalAnalysisController::deleteRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        post("/api/regional", RegionalAnalysisController::createRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
    }

    private static class WrappedURL implements Serializable {
        public String url;

        public WrappedURL(String url) {
            this.url = url;
        }
    }
}
