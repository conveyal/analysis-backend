package com.conveyal.taui.controllers;

import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.models.Modification;
import com.conveyal.taui.models.Scenario;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;

import static com.conveyal.taui.util.SparkUtil.haltWithJson;
import static spark.Spark.*;

/**
 * Controller for persisting modifications.
 */
public class ModificationController {
    private static final Logger LOG = LoggerFactory.getLogger(ModificationController.class);

    public static Modification getModification (Request req, Response res) {
        String id = req.params("id");
        return Persistence.modifications.get(id);
    }

    public static Modification createOrUpdate (Request req, Response res) {
        Modification mod = null;
        try {
            mod = JsonUtilities.objectMapper.readValue(req.body(), Modification.class);
        } catch (IOException e) {
            LOG.info("Error parsing modification JSON from client", e);
            haltWithJson(400, "Error parsing modification JSON from client");
        }

        Persistence.modifications.put(mod.id, mod);

        return mod;
    }

    public static Modification deleteModification (Request req, Response res) {
        Modification m = Persistence.modifications.get(req.params("id"));
        if (m == null) {
            haltWithJson(404, "Unable to delete modification. Modification does not exist.");
        }

        Scenario s = Persistence.scenarios.get(m.scenario);

        return Persistence.modifications.remove(m.id);
    }

    public static void register () {
        get("/api/modification/:id", ModificationController::getModification, JsonUtil.objectMapper::writeValueAsString);
        post("/api/modification", ModificationController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        // option to get any configured cors headers
        options("/api/modification", (q, s) -> "");
        put("/api/modification/:id", ModificationController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        options("/api/modification/:id", (q, s) -> "");
        delete("/api/modification/:id", ModificationController::deleteModification, JsonUtil.objectMapper::writeValueAsString);
    }
}
