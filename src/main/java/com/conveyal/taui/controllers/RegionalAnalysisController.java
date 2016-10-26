package com.conveyal.taui.controllers;

import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.analysis.RegionalAnalysisManager;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.conveyal.taui.grids.SeamlessCensusGridFetcher.ZOOM;

import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Created by matthewc on 10/21/16.
 */
public class RegionalAnalysisController {
    public static List<RegionalAnalysis> getRegionalAnalysis (Request req, Response res) {
        String projectId = req.params("projectId");
        return Persistence.regionalAnalyses.values().stream()
                .filter(q -> projectId.equals(q.projectId))
                .collect(Collectors.toList());
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
        post("/api/analysis", RegionalAnalysisController::createRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
    }
}
