package com.conveyal.taui.controllers;

import com.conveyal.osmlib.OSM;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.grids.SeamlessCensusGridExtractor;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.persistence.OSMPersistence;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Created by matthewc on 7/12/16.
 */
public class ProjectController {
    private static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);
    private static final FileItemFactory fileItemFactory = new DiskFileItemFactory();

    public static Project getProject (Request req, Response res) {
        return Persistence.projects.findByIdFromRequestIfPermitted(req);
    }

    public static Collection<Project> getAllProjects (Request req, Response res) {
        return Persistence.projects.findAllForRequest(req);
    }

    public static Map<String, List<FileItem>> getFilesFromRequest (Request req) {
        try {
            ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
            return sfu.parseParameterMap(req.raw());
        } catch (FileUploadException e) {
            throw AnalysisServerException.FileUpload("Error uploading files. " + e.getMessage());
        }
    }

    public static Project getProjectFromFiles(Map<String, List<FileItem>> files) {
        try {
            InputStream is = files.get("project").get(0).getInputStream();
            final Project project = JsonUtil.objectMapper.readValue(is, Project.class);
            is.close();

            return project;
        } catch (IOException e) {
            throw AnalysisServerException.BadRequest("Error parsing project. " + e.getMessage());
        }
    }

    private static synchronized OSM fetchOsm (Project project) throws Exception {
        // Set the project status
        project.statusCode = Project.StatusCode.DOWNLOADING_OSM;
        project = Persistence.projects.put(project);

        // Retrieve and save the OSM for the project bounds at the given _id
        return OSMPersistence.retrieveOSMFromVexForBounds(project.bounds, project._id);
        // TODO remove all cached transport networks for this project
    }

    private static synchronized List<Project.OpportunityDataset> fetchCensus (Project project) throws Exception {
        // Set the project status
        project.statusCode = Project.StatusCode.DOWNLOADING_CENSUS;
        project = Persistence.projects.put(project);
        return SeamlessCensusGridExtractor.retrieveAndExtractCensusDataForBounds(project.bounds, project._id);
    }

    public static void saveCustomOSM(Project project, Map<String, List<FileItem>> files) throws Exception {
        project.customOsm = true;
        File customOsmData = File.createTempFile("uploaded-osm", ".pbf");
        files.get("customOpenStreetMapData").get(0).write(customOsmData);
        OSMPersistence.cache.put(project._id, customOsmData);
        customOsmData.delete();
    }

    public static void fetchOsmAndCensusDataInThread (String _id, Map<String, List<FileItem>> files, boolean newBounds) {
        boolean customOsm = files.containsKey("customOpenStreetMapData");
        new Thread(() -> {
            Project project = Persistence.projects.get(_id);

            try {
                if (customOsm) {
                    saveCustomOSM(project, files);
                }

                if (newBounds) {
                    if (!customOsm) fetchOsm(project);
                    fetchCensus(project);
                }

                project.statusCode = Project.StatusCode.DONE;
                Persistence.projects.put(project);
            } catch (Exception e) {
                project.statusCode = Project.StatusCode.ERROR;
                project.statusMessage = "Error while fetching data. " + e.getMessage();
                Persistence.projects.put(project);

                LOG.error("Error while fetching OSM. " + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }

    public static Project create(Request req, Response res) {
        final Map<String, List<FileItem>> files = getFilesFromRequest(req);
        Project project = getProjectFromFiles(files);

        // Set the `accessGroup` and `createdBy`
        project.accessGroup = req.attribute("accessGroup");
        project.createdBy = req.attribute("email");

        // Set the status to Started
        project.statusCode = Project.StatusCode.STARTED;

        // Create the project
        Persistence.projects.create(project);

        // Fetch data and update the statuses separately
        fetchOsmAndCensusDataInThread(project._id, files, true);

        return project;
    }

    public static Project update(Request req, Response res) {
        final Project existingProject = Persistence.projects.findByIdFromRequestIfPermitted(req);
        final Map<String, List<FileItem>> files = getFilesFromRequest(req);
        Project project = getProjectFromFiles(files);

        boolean boundsChanged = !existingProject.bounds.equals(project.bounds, 1e-6);
        boolean customOSM = files.containsKey("customOpenStreetMapData");

        // Set updatedBy
        project.updatedBy = req.attribute("email");

        if (boundsChanged || customOSM) {
            project.statusCode = Project.StatusCode.STARTED;
            project = Persistence.projects.put(project);

            // Fetch data and update the statuses separately
            fetchOsmAndCensusDataInThread(project._id, files, boundsChanged);
        }

        return project;
    }

    public static Project deleteProject (Request req, Response res) {
        return Persistence.projects.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));
    }

    public static void register () {
        get("/api/project", ProjectController::getAllProjects, JsonUtil.objectMapper::writeValueAsString);
        get("/api/project/:_id", ProjectController::getProject, JsonUtil.objectMapper::writeValueAsString);
        get("/api/project/:project/scenarios", ScenarioController::getAllScenarios, JsonUtil.objectMapper::writeValueAsString);
        get("/api/project/:project/bookmark", BookmarkController::getAllBookmarks, JsonUtil.objectMapper::writeValueAsString);
        post("/api/project/:project/bookmark", BookmarkController::createBookmark, JsonUtil.objectMapper::writeValueAsString);
        post("/api/project", ProjectController::create, JsonUtil.objectMapper::writeValueAsString);
        put("/api/project/:_id", ProjectController::update, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/project/:_id", ProjectController::deleteProject, JsonUtil.objectMapper::writeValueAsString);
    }
}
