package com.conveyal.taui.controllers;

import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.models.Modification;
import com.conveyal.taui.models.Scenario;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.conveyal.taui.util.SparkUtil.haltWithJson;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Controller for scenarios.
 */
public class ScenarioController {
    private static Scenario getAndHaltIfMissing (String id) {
        Scenario scenario = Persistence.scenarios.get(id);
        if (scenario == null) {
            haltWithJson(404, "Scenario ID " + id + " does not exist.");
        }
        return scenario;
    }

    public static Scenario getScenario (Request req, Response res) {
        String id = req.params("id");
        String group = (String) req.attribute("group");
        Scenario scenario = getAndHaltIfMissing(id);

        return scenario;
    }

    public static Collection<Scenario> getAllScenarios (Request req, Response res) {
        String project = req.params("project");
        return Persistence.scenarios.values().stream()
                .filter(s -> project.equals(s.projectId))
                .collect(Collectors.toList());
    }

    public static Scenario createOrUpdate (Request req, Response res) {
        Scenario scenario = null;
        try {
            scenario = JsonUtilities.objectMapper.readValue(req.body(), Scenario.class);
        } catch (IOException e) {
            haltWithJson(400, "Unable to parse Scenario JSON sent from the client.");
        }

        Scenario existing = Persistence.scenarios.get(scenario.id);

        if (existing != null && !scenario.projectId.equals(existing.projectId)) {
            haltWithJson(403, "Cannot change a Scenario's project.");
        }

        Persistence.scenarios.put(scenario.id, scenario);

        return scenario;
    }

    public static Collection<Modification> modifications (Request req, Response res) {
        return Persistence.modifications.getByProperty("scenario", req.params("id"));
    }

    public static Scenario deleteScenario (Request req, Response res) {
        return Persistence.scenarios.remove(req.params("id"));
    }

    public static void register () {
        get("/api/scenario/:id", ScenarioController::getScenario, JsonUtil.objectMapper::writeValueAsString);
        get("/api/scenario/:id/modifications", ScenarioController::modifications, JsonUtil.objectMapper::writeValueAsString);
        post("/api/scenario", ScenarioController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        options("/api/scenario", (q, s) -> "");
        put("/api/scenario/:id", ScenarioController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/scenario/:id", ScenarioController::deleteScenario, JsonUtil.objectMapper::writeValueAsString);
        options("/api/scenario/:id", (q, s) -> "");
    }
}
