package com.conveyal.taui.controllers;

import com.conveyal.taui.models.AddTripPattern;
import com.conveyal.taui.models.ConvertToFrequency;
import com.conveyal.taui.models.Modification;
import com.conveyal.taui.models.Scenario;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.mongodb.QueryBuilder;
import org.bson.types.ObjectId;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
                QueryBuilder.start("scenarioId").is(req.params("_id")).get(),
                req.attribute("accessGroup")
        );
    }

    public static Collection<Modification> importModifications (Request req, Response res) {
        String importId = req.params("_importId");
        String newId = req.params("_id");
        Collection<Modification> modifications = Persistence.modifications.findPermitted(
                QueryBuilder.start("scenarioId").is(importId).get(),
                req.attribute("accessGroup")
        );

        // This would be a lot easier if we just used the actual `_id`s and dealt with it elsewhere when searching. They
        // should be unique anyways. Hmmmmmmmmmmmm. Tradeoffs.
        // Need to make two passes to create all the pairs and rematch for phasing
        Map<String, String> modificationIdPairs = new HashMap<>();
        Map<String, String> timetableIdPairs = new HashMap<>();

        return modifications
                .stream()
                .map(modification -> {
                    String oldModificationId = modification._id;
                    Modification clone = Persistence.modifications.create(modification);
                    modificationIdPairs.put(oldModificationId, clone._id);

                    // Change the scenarioId, most important part!
                    clone.scenarioId = newId;

                    // Set `name` to include "(import)"
                    clone.name = clone.name + " (import)";

                    // Set `updatedBy` by manually, `createdBy` stays with the original author
                    clone.updatedBy = req.attribute("email");

                    // Matched up the phased entries and timetables
                    if (modification.getType().equals(AddTripPattern.type)) {
                        ((AddTripPattern) clone).timetables.forEach(tt -> {
                            String oldTTId = tt._id;
                            tt._id = new ObjectId().toString();
                            timetableIdPairs.put(oldTTId, tt._id);
                        });
                    } else if (modification.getType().equals(ConvertToFrequency.type)) {
                        ((ConvertToFrequency) clone).entries.forEach(tt -> {
                            String oldTTId = tt._id;
                            tt._id = new ObjectId().toString();
                            timetableIdPairs.put(oldTTId, tt._id);
                        });
                    }

                    return clone;
                })
                .collect(Collectors.toList())
                .stream()
                .map(modification -> {
                    if (modification.getType().equals(AddTripPattern.type)) {
                        ((AddTripPattern) modification).timetables.forEach(tt -> {
                            String pft = tt.phaseFromTimetable;
                            if (pft != null && pft.length() > 0) {
                                String[] pfts = pft.split(":");
                                tt.phaseFromTimetable = modificationIdPairs.get(pfts[0]) + ":" + timetableIdPairs.get(pfts[1]);
                            }
                        });
                    } else if (modification.getType().equals(ConvertToFrequency.type)) {
                        ((ConvertToFrequency) modification).entries.forEach(tt -> {
                            String pft = tt.phaseFromTimetable;
                            if (pft != null && pft.length() > 0) {
                                String[] pfts = pft.split(":");
                                tt.phaseFromTimetable = modificationIdPairs.get(pfts[0]) + ":" + timetableIdPairs.get(pfts[1]);
                            }
                        });
                    }

                    return Persistence.modifications.put(modification);
                })
                .collect(Collectors.toList());
    }

    public static Scenario deleteScenario (Request req, Response res) {
        return Persistence.scenarios.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));
    }

    public static void register () {
        get("/api/scenario/:_id", ScenarioController::findById, JsonUtil.objectMapper::writeValueAsString);
        get("/api/scenario/:_id/modifications", ScenarioController::modifications, JsonUtil.objectMapper::writeValueAsString);
        post("/api/scenario/:_id/import/:_importId", ScenarioController::importModifications, JsonUtil.objectMapper::writeValueAsString);
        post("/api/scenario", ScenarioController::create, JsonUtil.objectMapper::writeValueAsString);
        options("/api/scenario", (q, s) -> "");
        put("/api/scenario/:_id", ScenarioController::update, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/scenario/:_id", ScenarioController::deleteScenario, JsonUtil.objectMapper::writeValueAsString);
        options("/api/scenario/:_id", (q, s) -> "");
    }
}
