package com.conveyal.taui.controllers;

import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.models.Modification;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.fasterxml.jackson.core.JsonParseException;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;

import static spark.Spark.halt;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;
import static spark.Spark.options;

/**
 * Controller for persisting modifications.
 */
public class ModificationController {
    public static Modification getModification (Request req, Response res) {
        String id = req.params("id");
        return Persistence.modifications.get(id);
    }

    public static Modification createOrUpdate (Request req, Response res) {
        Modification mod = null;
        try {
            mod = JsonUtilities.objectMapper.readValue(req.body(), Modification.class);
        } catch (IOException e) {
            halt(400, "Bad modification");
        }

        Persistence.modifications.put(mod.id, mod);

        return mod;
    }

    public static void register () {
        get("/modification/:id", ModificationController::getModification, JsonUtil.objectMapper::writeValueAsString);
        post("/modification", ModificationController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        // option to get any configured cors headers
        options("/modification", (q, s) -> "");
        put("/modification/:id", ModificationController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        options("/modification/:id", (q, s) -> "");
    }
}
