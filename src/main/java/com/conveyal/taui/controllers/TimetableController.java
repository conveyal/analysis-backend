package com.conveyal.taui.controllers;

import com.conveyal.taui.models.AddTripPattern;
import com.conveyal.taui.models.Modification;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.Region;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.mongodb.QueryBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.Collection;
import java.util.List;

import static spark.Spark.get;

/**
 * Created by evan siroky on 5/3/18.
 */
public class TimetableController {
    private static final Logger LOG = LoggerFactory.getLogger(TimetableController.class);

    public static String getTimetables (Request req, Response res) {
        JSONArray json = new JSONArray();
        Collection<Region> regions = Persistence.regions.findAllForRequest(req);

        for (Region region : regions) {
            JSONObject r = new JSONObject();
            r.put("_id", region._id);
            r.put("name", region.name);
            JSONArray regionProjects = new JSONArray();
            List<Project> projects = Persistence.projects.find(QueryBuilder.start("regionId").is(region._id).get()).toArray();
            for (Project project : projects) {
                JSONObject p = new JSONObject();
                p.put("_id", project._id);
                p.put("name", project.name);
                JSONArray projectModifications = new JSONArray();
                List<Modification> modifications = Persistence.modifications.find(
                        QueryBuilder.start("projectId").is(project._id).and("type").is("add-trip-pattern").get()
                ).toArray();
                for (Modification modification : modifications) {
                    AddTripPattern tripPattern = (AddTripPattern) modification;
                    JSONObject m = new JSONObject();
                    m.put("_id", modification._id);
                    m.put("name", modification.name);
                    m.put("segments", JsonUtil.objectMapper.valueToTree(tripPattern.segments));
                    JSONArray modificationTimetables = new JSONArray();
                    for (AddTripPattern.Timetable timetable : tripPattern.timetables) {
                        modificationTimetables.add(JsonUtil.objectMapper.valueToTree(timetable));
                    }
                    m.put("timetables", modificationTimetables);
                    if (modificationTimetables.size() > 0) {
                        projectModifications.add(m);
                    }
                }
                p.put("modifications", projectModifications);
                if (projectModifications.size() > 0) {
                    regionProjects.add(p);
                }
            }
            r.put("projects", regionProjects);
            if (regionProjects.size() > 0) {
                json.add(r);
            }
        }

        return json.toString();
    }

    public static void register () {
        get("/api/timetables", TimetableController::getTimetables);
    }
}
