package com.conveyal.taui.controllers;

import com.conveyal.r5.util.ExceptionUtils;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.models.Region;
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
public class RegionController {
    private static final Logger LOG = LoggerFactory.getLogger(RegionController.class);
    private static final FileItemFactory fileItemFactory = new DiskFileItemFactory();

    public static Region getRegion (Request req, Response res) {
        return Persistence.regions.findByIdFromRequestIfPermitted(req);
    }

    public static Collection<Region> getAllRegions (Request req, Response res) {
        return Persistence.regions.findAllForRequest(req);
    }

    public static Map<String, List<FileItem>> getFilesFromRequest (Request req) {
        try {
            ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
            return sfu.parseParameterMap(req.raw());
        } catch (FileUploadException e) {
            throw AnalysisServerException.fileUpload("Error uploading files. " + ExceptionUtils.asString(e));
        }
    }

    /**
     * Read OSM files supplied during region creation
     * @param region
     * @param files MIME multipart upload (including osm.pbf)
     * @throws Exception
     */
    public static void uploadOSM(Region region, Map<String, List<FileItem>> files) {
        try {
            // Set the status to Started
            region.statusCode = Region.StatusCode.STARTED;
            Persistence.regions.put(region);

            File customOsmData = File.createTempFile("uploaded-osm", ".pbf");
            files.get("customOpenStreetMapData").get(0).write(customOsmData);
            OSMPersistence.cache.put(region._id, customOsmData);
            customOsmData.delete();

            region.customOsm = true;
            region.statusCode = Region.StatusCode.DONE;
            Persistence.regions.put(region);

        } catch (Exception e) {
            Persistence.regions.removeIfPermitted(region._id, region.accessGroup);
            throw AnalysisServerException.fileUpload("Error processing OSM uploaded during region creation. " +
                    ExceptionUtils.asString(e));
        }

    }

    /**
     * Create a region with OSM.  //TODO move OSM upload/selection to explicit TransportNetwork creation
     * @param req body contains MIME multipart upload, which in turn contains region (.json representation) and OSM (
     *            .pbf)
     * @param res
     * @return
     * @throws Exception
     */
    public static Region create(Request req, Response res) throws Exception {
        try {
            final Map<String, List<FileItem>> files = getFilesFromRequest(req);

            InputStream is = files.get("region").get(0).getInputStream();
            final Region region = JsonUtil.objectMapper.readValue(is, Region.class);
            is.close();

            // Set the `accessGroup` and `createdBy`
            region.accessGroup = req.attribute("accessGroup");
            region.createdBy = req.attribute("email");

            // Create the region
            Persistence.regions.create(region);

            // Upload custom OSM data
            uploadOSM(region, files);

            return region;
        } catch (IOException e) {
            throw AnalysisServerException.badRequest("Error parsing region. " + ExceptionUtils.asString(e));
        }
    }

    public static Region update(Request req, Response res) throws Exception {
        return Persistence.regions.updateFromJSONRequest(req);
    }

    public static Region deleteRegion (Request req, Response res) {
        return Persistence.regions.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));
    }

    public static void register () {
        get("/api/region", RegionController::getAllRegions, JsonUtil.objectMapper::writeValueAsString);
        get("/api/region/:_id", RegionController::getRegion, JsonUtil.objectMapper::writeValueAsString);
        get("/api/region/:region/projects", ProjectController::getAllProjects, JsonUtil.objectMapper::writeValueAsString);
        get("/api/region/:region/bookmark", BookmarkController::getAllBookmarks, JsonUtil.objectMapper::writeValueAsString);
        post("/api/region/:region/bookmark", BookmarkController::createBookmark, JsonUtil.objectMapper::writeValueAsString);
        post("/api/region", RegionController::create, JsonUtil.objectMapper::writeValueAsString);
        put("/api/region/:_id", RegionController::update, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/region/:_id", RegionController::deleteRegion, JsonUtil.objectMapper::writeValueAsString);
    }
}
