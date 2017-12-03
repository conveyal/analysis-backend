package com.conveyal.taui.controllers;

import com.conveyal.taui.models.AbstractTimetable;
import com.conveyal.taui.models.AddTripPattern;
import com.conveyal.taui.models.ConvertToFrequency;
import com.conveyal.taui.models.Modification;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import org.bson.types.ObjectId;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static void mapPhaseIds (List<AbstractTimetable> timetables, String oldModificationId, String newModificationId) {
        Map<String, String> idPairs = new HashMap<String, String>();
        timetables.forEach(tt -> {
            String newId = ObjectId.get().toString();
            idPairs.put(tt._id, newId);
            tt._id = newId;
        });

        timetables
                .stream()
                .filter(tt -> tt.phaseFromTimetable != null && tt.phaseFromTimetable.length() > 0)
                .filter(tt -> tt.phaseFromTimetable.contains(oldModificationId))
                .forEach(tt -> {
                    String oldTTId = tt.phaseFromTimetable.split(":")[1];
                    tt.phaseFromTimetable = newModificationId + ":" + idPairs.get(oldTTId);
                });
    }

    public static Modification copyModification (Request req, Response res) {
        Modification modification = Persistence.modifications.findByIdFromRequestIfPermitted(req);

        String oldId = modification._id;
        Modification clone = Persistence.modifications.create(modification);

        // Matched up the phased entries and timetables
        if (modification.getType().equals(AddTripPattern.type)) {
            mapPhaseIds((List<AbstractTimetable>)(List<?>)((AddTripPattern) clone).timetables, oldId, clone._id);
        } else if (modification.getType().equals(ConvertToFrequency.type)) {
            mapPhaseIds((List<AbstractTimetable>)(List<?>)((ConvertToFrequency) clone).entries, oldId, clone._id);
        }

        // Set `name` to include "(copy)"
        clone.name = clone.name + " (copy)";

        // Set `updateBy` manually, `createdBy` stays with the original modification author
        clone.updatedBy = req.attribute("email");

        // Update the clone
        return Persistence.modifications.put(clone);
    }

    public static void register () {
        get("/api/modification/:_id", ModificationController::getModification, JsonUtil.objectMapper::writeValueAsString);
        post("/api/modification/:_id/copy", ModificationController::copyModification, JsonUtil.objectMapper::writeValueAsString);
        post("/api/modification", ModificationController::create, JsonUtil.objectMapper::writeValueAsString);
        // option to get any configured cors headers
        options("/api/modification", (q, s) -> "");
        put("/api/modification/:_id", ModificationController::update, JsonUtil.objectMapper::writeValueAsString);
        options("/api/modification/:_id", (q, s) -> "");
        delete("/api/modification/:_id", ModificationController::deleteModification, JsonUtil.objectMapper::writeValueAsString);
    }
}
