package com.conveyal.taui.controllers;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.PercentileGridSampler;
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

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static com.conveyal.taui.grids.SeamlessCensusGridFetcher.ZOOM;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static spark.Spark.get;
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

    /** Get a particular percentile of a query as a grid file */
    public static Object getPercentile (Request req, Response res) throws IOException {
        String regionalAnalysisId = req.params("regionalAnalysisId");
        // while we can do non-integer percentiles, don't allow that here to prevent cache misses
        int percentile = parseInt(req.params("percentile"));
        String percentileGridKey = String.format("%s_%d_percentile.grid", regionalAnalysisId, percentile);
        String accessGridKey = String.format("%s.access", regionalAnalysisId);

        if (!s3.doesObjectExist(AnalystConfig.resultsBucket, percentileGridKey)) {
            // make the grid
            LOG.info("Percentile {} for regional analysis {} not found, building it", percentile, regionalAnalysisId);
            Grid grid =
                    PercentileGridSampler.computePercentile(AnalystConfig.resultsBucket, accessGridKey, percentile);

            PipedInputStream pis = new PipedInputStream();
            PipedOutputStream pos = new PipedOutputStream(pis);

            ObjectMetadata om = new ObjectMetadata();
            om.setContentType("application/octet-stream");
            om.setContentEncoding("gzip");

            executorService.execute(() -> {
                try {
                    grid.write(new GZIPOutputStream(pos));
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

        res.redirect(url.toString());
        res.status(302); // temporary redirect, this URL will soon expire
        return null;
    }

    public static RegionalAnalysis createRegionalAnalysis (Request req, Response res) throws IOException {
        RegionalAnalysis regionalAnalysis = JsonUtil.objectMapper.readValue(req.body(), RegionalAnalysis.class);

        Bundle bundle = Persistence.bundles.get(regionalAnalysis.bundleId);
        Project project = Persistence.projects.get(bundle.projectId);

        // fill in the fields
        regionalAnalysis.zoom = ZOOM;
        regionalAnalysis.west = Grid.lonToPixel(project.bounds.west, regionalAnalysis.zoom);
        regionalAnalysis.north = Grid.latToPixel(project.bounds.north, regionalAnalysis.zoom);
        regionalAnalysis.width = Grid.lonToPixel(project.bounds.east, regionalAnalysis.zoom) - regionalAnalysis.west + 1; // + 1 to account for flooring
        regionalAnalysis.height = Grid.latToPixel(project.bounds.south, regionalAnalysis.zoom) - regionalAnalysis.north + 1;

        regionalAnalysis.id = UUID.randomUUID().toString();
        regionalAnalysis.projectId = project.id;

        // this scenario is specific to this job
        regionalAnalysis.request.scenarioId = null;
        regionalAnalysis.request.scenario.id = regionalAnalysis.id;

        regionalAnalysis.creationTime = System.currentTimeMillis();

        Persistence.regionalAnalyses.put(regionalAnalysis.id, regionalAnalysis);
        RegionalAnalysisManager.enqueue(regionalAnalysis);

        return regionalAnalysis;
    }

    public static void register () {
        get("/api/project/:projectId/regional", RegionalAnalysisController::getRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:regionalAnalysisId/grid/:percentile", RegionalAnalysisController::getPercentile);
        post("/api/regional", RegionalAnalysisController::createRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
    }
}
