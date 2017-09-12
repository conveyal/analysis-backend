package com.conveyal.taui.controllers;

import com.conveyal.taui.models.Project;
import com.conveyal.taui.persistence.OSMPersistence;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.taui.util.SparkUtil.haltWithJson;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Created by matthewc on 7/12/16.
 */
public class ProjectController {
    private static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);

    private static FileItemFactory fileItemFactory = new DiskFileItemFactory();

    public static Project getProject (Request req, Response res) {
        Project project = Persistence.projects.get(req.params("id"));

        if (project == null) {
            haltWithJson(404, "Project does not exist for ID: " + req.params("id") + ".");
        }

        return project;
    }

    public static List<Project> getAllProjects (Request req, Response res) {
        String group = req.attribute("group");
        return Persistence.projects.values().stream()
                .filter(p -> group.equals(p.group))
                .collect(Collectors.toList());
    }

    public static Project createOrUpdate (Request req, Response res) throws Exception {
        Project project = null;
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        Map<String, List<FileItem>> files = sfu.parseParameterMap(req.raw());

        try {
            InputStream is = files.get("project").get(0).getInputStream();
            project = JsonUtil.objectMapper.readValue(is, Project.class);
            is.close();
        } catch (Exception e) {
            LOG.error("Could not deserialize project from client", e);
            haltWithJson(400, "Could not deserialize project from client");
        }

        project.group = req.attribute("group");

        Project existing = Persistence.projects.get(project.id);

        if (existing != null && !existing.group.equals(project.group)) {
            haltWithJson(401, "Attempt to overwrite existing project belonging to other user");
        }

        Persistence.projects.put(project.id, project);

        final Project finalProject = project.clone();
        if (files.containsKey("customOpenStreetMapData")) {
            // parse uploaded OSM
            new Thread(() -> {
                try {
                    File customOsmData = File.createTempFile("uploaded-osm", ".pbf");
                    files.get("customOpenStreetMapData").get(0).write(customOsmData);
                    OSMPersistence.cache.put(finalProject.id, customOsmData);
                    customOsmData.delete();

                    // update load status
                    Project.loadStatusForProject.remove(finalProject.id);

                    if (existing == null || !existing.bounds.equals(finalProject.bounds, 1e-6)) {
                        // fetch census data
                        finalProject.fetchCensus();
                        // save indicators
                        Persistence.projects.put(finalProject.id, finalProject);
                    }
                } catch (Exception e) {
                    LOG.error("Exception storing OSM", e);
                }
            }).start();
        }
        else if (existing == null || !existing.bounds.equals(project.bounds, 1e-6)) {
            // download OSM
            // TODO how to feed osm download errors back to user?
            // how to prevent project from being used before OSM is downloaded?
            new Thread(() -> {
                try {
                    finalProject.fetchOsm();
                    finalProject.fetchCensus();
                    Project.loadStatusForProject.remove(finalProject.id);
                    // save indicators
                    Persistence.projects.put(finalProject.id, finalProject);
                } catch (IOException e) {
                    LOG.error("Exception fetching OSM", e);
                }
            }).start();
        }

        return project;
    }

    public static Project deleteProject (Request req, Response res) {
        String id = req.params("id");
        Project project = Persistence.projects.get(id);
        if (project == null) haltWithJson(404, "Project ID: " + id + " does not exist on the server.");

        return Persistence.projects.remove(id);
    }

    public static void register () {
        get("/api/project", ProjectController::getAllProjects, JsonUtil.objectMapper::writeValueAsString);
        get("/api/project/:id", ProjectController::getProject, JsonUtil.objectMapper::writeValueAsString);
        get("/api/project/:project/scenarios", ScenarioController::getAllScenarios, JsonUtil.objectMapper::writeValueAsString);
        get("/api/project/:project/bookmark", BookmarkController::getAllBookmarks, JsonUtil.objectMapper::writeValueAsString);
        post("/api/project/:project/bookmark", BookmarkController::createBookmark, JsonUtil.objectMapper::writeValueAsString);
        post("/api/project", ProjectController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        put("/api/project/:id", ProjectController::createOrUpdate, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/project/:id", ProjectController::deleteProject, JsonUtil.objectMapper::writeValueAsString);
    }
}
