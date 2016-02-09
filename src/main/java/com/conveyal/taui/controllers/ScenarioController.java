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

import static spark.Spark.*;

/**
 * Controller for scenarios.
 */
public class ScenarioController {
    public static Scenario getScenario (Request req, Response res) {
        String id = req.params("id");
        return Persistence.scenarios.get(id);
    }

    public static Collection<Scenario> getAllScenarios (Request req, Response res) {
        return Persistence.scenarios.values();
    }

    public static Scenario createOrUpdate (Request req, Response res) {
        Scenario scenario = null;
        try {
            scenario = JsonUtilities.objectMapper.readValue(req.body(), Scenario.class);
        } catch (IOException e) {
            halt(400, "Bad modification");
        }

        Persistence.scenarios.put(scenario.id, scenario);

        return scenario;
    }

    public static Collection<Modification> modifications (Request req, Response res) {
        String id = req.params("id");
        Scenario scenario = Persistence.scenarios.get(id);
        return scenario.modifications.stream().map(Persistence.modifications::get).collect(Collectors.toList());
    }

    public static void register () {
        get("/scenario/:id", ScenarioController::getScenario, JsonUtil.objectMapper::writeValueAsString);
        get("/scenario", ScenarioController::getAllScenarios, JsonUtil.objectMapper::writeValueAsString);
        get("/scenario/:id/modifications", ScenarioController::modifications, JsonUtil.objectMapper::writeValueAsString);
        post("/scenario/", ScenarioController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        options("/scenario/", (q, s) -> "");
        put("/scenario/:id", ScenarioController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        options("/scenario/:id", (q, s) -> "");
    }
}
