package com.conveyal.taui.controllers;

import com.conveyal.taui.models.Modification;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import spark.Request;
import spark.Response;

import java.io.IOException;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Controller for persisting modifications.
 */
public class ModificationController {
    public static Modification getModification (Request req, Response res) {
        return Persistence.modifications.findByIdFromRequestIfPermitted(req);
    }

    public static Modification create (Request request, Response response) throws IOException {
        return Persistence.modifications.createFromJSONRequest(request, Modification.class);
    }

    public static Modification update (Request request, Response response) throws IOException {
        return Persistence.modifications.updateFromJSONRequest(request, Modification.class);
    }

    public static Modification deleteModification (Request req, Response res) {
        return Persistence.modifications.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));
    }

    public static void register () {
        get("/api/modification/:_id", ModificationController::getModification, JsonUtil.objectMapper::writeValueAsString);
        post("/api/modification", ModificationController::create, JsonUtil.objectMapper::writeValueAsString);
        // option to get any configured cors headers
        options("/api/modification", (q, s) -> "");
        put("/api/modification/:_id", ModificationController::update, JsonUtil.objectMapper::writeValueAsString);
        options("/api/modification/:_id", (q, s) -> "");
        delete("/api/modification/:_id", ModificationController::deleteModification, JsonUtil.objectMapper::writeValueAsString);
    }
}
