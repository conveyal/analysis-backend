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
        String group = (String) req.attribute("group");
        Scenario scenario = Persistence.scenarios.get(id);

        if (scenario == null || !group.equals(scenario.group)) halt(404);

        return scenario;
    }

    public static Collection<Scenario> getAllScenarios (Request req, Response res) {
        String group = (String) req.attribute("group");
        return Persistence.scenarios.values().stream()
                .filter(s -> group.equals(s.group))
                .collect(Collectors.toList());
    }

    public static Scenario createOrUpdate (Request req, Response res) {
        Scenario scenario = null;
        try {
            scenario = JsonUtilities.objectMapper.readValue(req.body(), Scenario.class);
        } catch (IOException e) {
            halt(400, "Bad modification");
        }

        scenario.group = (String) req.attribute("group");

        Scenario existing = Persistence.scenarios.get(scenario.id);

        if (existing != null && !scenario.group.equals(existing.group)) {
            halt(403);
        }

        Persistence.scenarios.put(scenario.id, scenario);

        return scenario;
    }

    public static Collection<Modification> modifications (Request req, Response res) {
        String id = req.params("id");
        Scenario scenario = Persistence.scenarios.get(id);

        if (scenario == null) halt(404);

        // 404 don't give any information as to whether it exists or not
        if (!req.attribute("group").equals(scenario.group)) halt(404);

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
