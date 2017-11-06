package com.conveyal.taui.controllers;

import com.conveyal.taui.models.Modification;
import com.conveyal.taui.models.Scenario;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.mongodb.QueryBuilder;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Controller for scenarios.
 */
public class ScenarioController {
    public static Scenario findById(Request req, Response res) {
        return Persistence.scenarios.findByIdFromRequestIfPermitted(req);
    }

    public static Collection<Scenario> getAllScenarios (Request req, Response res) {
        return Persistence.scenarios.findPermitted(
                QueryBuilder.start("projectId").is(req.params("project")).get(),
                req.attribute("accessGroup")
        );
    }

    public static Scenario create(Request req, Response res) throws IOException {
        return Persistence.scenarios.createFromJSONRequest(req, Scenario.class);
    }

    public static Scenario update(Request req, Response res) throws IOException {
        return Persistence.scenarios.updateFromJSONRequest(req, Scenario.class);
    }

    public static Collection<Modification> modifications (Request req, Response res) {
        return Persistence.modifications.findPermitted(
                QueryBuilder.start("scenarioId").is((req.params("_id"))).get(),
                req.attribute("accessGroup")
        );
    }

    public static Scenario deleteScenario (Request req, Response res) {
        return Persistence.scenarios.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));
    }

    public static void register () {
        get("/api/scenario/:_id", ScenarioController::findById, JsonUtil.objectMapper::writeValueAsString);
        get("/api/scenario/:_id/modifications", ScenarioController::modifications, JsonUtil.objectMapper::writeValueAsString);
        post("/api/scenario", ScenarioController::create, JsonUtil.objectMapper::writeValueAsString);
        options("/api/scenario", (q, s) -> "");
        put("/api/scenario/:_id", ScenarioController::update, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/scenario/:_id", ScenarioController::deleteScenario, JsonUtil.objectMapper::writeValueAsString);
        options("/api/scenario/:_id", (q, s) -> "");
    }
}
