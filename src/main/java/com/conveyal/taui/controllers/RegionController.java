package com.conveyal.taui.controllers;

import com.conveyal.osmlib.OSM;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.grids.SeamlessCensusGridExtractor;
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
            throw AnalysisServerException.FileUpload("Error uploading files. " + e.getMessage());
        }
    }

    public static Region getRegionFromFiles(Map<String, List<FileItem>> files) {
        try {
            InputStream is = files.get("region").get(0).getInputStream();
            final Region region = JsonUtil.objectMapper.readValue(is, Region.class);
            is.close();

            return region;
        } catch (IOException e) {
            throw AnalysisServerException.BadRequest("Error parsing region. " + e.getMessage());
        }
    }

    private static synchronized OSM fetchOsm (Region region) throws Exception {
        // Set the region status
        region.statusCode = Region.StatusCode.DOWNLOADING_OSM;
        region = Persistence.regions.put(region);

        // Retrieve and save the OSM for the region bounds at the given _id
        return OSMPersistence.retrieveOSMFromVexForBounds(region.bounds, region._id);
        // TODO remove all cached transport networks for this region
    }

    private static synchronized List<Region.OpportunityDataset> fetchCensus (Region region) throws Exception {
        // Set the region status
        region.statusCode = Region.StatusCode.DOWNLOADING_CENSUS;
        region = Persistence.regions.put(region);
        return SeamlessCensusGridExtractor.retrieveAndExtractCensusDataForBounds(region.bounds, region._id);
    }

    public static void saveCustomOSM(Region region, Map<String, List<FileItem>> files) throws Exception {
        region.customOsm = true;
        File customOsmData = File.createTempFile("uploaded-osm", ".pbf");
        files.get("customOpenStreetMapData").get(0).write(customOsmData);
        OSMPersistence.cache.put(region._id, customOsmData);
        customOsmData.delete();
    }

    public static void fetchOsmAndCensusDataInThread (String _id, Map<String, List<FileItem>> files, boolean newBounds) {
        boolean customOsm = files.containsKey("customOpenStreetMapData");
        new Thread(() -> {
            Region region = Persistence.regions.get(_id);

            try {
                if (customOsm) {
                    saveCustomOSM(region, files);
                }

                if (newBounds) {
                    if (!customOsm) fetchOsm(region);
                    fetchCensus(region);
                }

                region.statusCode = Region.StatusCode.DONE;
                Persistence.regions.put(region);
            } catch (Exception e) {
                region.statusCode = Region.StatusCode.ERROR;
                region.statusMessage = "Error while fetching data. " + e.getMessage();
                Persistence.regions.put(region);

                LOG.error("Error while fetching OSM. " + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }

    public static Region create(Request req, Response res) {
        final Map<String, List<FileItem>> files = getFilesFromRequest(req);
        Region region = getRegionFromFiles(files);

        // Set the `accessGroup` and `createdBy`
        region.accessGroup = req.attribute("accessGroup");
        region.createdBy = req.attribute("email");

        // Set the status to Started
        region.statusCode = Region.StatusCode.STARTED;

        // Create the region
        Persistence.regions.create(region);

        // Fetch data and update the statuses separately
        fetchOsmAndCensusDataInThread(region._id, files, true);

        return region;
    }

    public static Region update(Request req, Response res) {
        final Region existingRegion = Persistence.regions.findByIdFromRequestIfPermitted(req);
        final Map<String, List<FileItem>> files = getFilesFromRequest(req);
        Region region = getRegionFromFiles(files);

        boolean boundsChanged = !existingRegion.bounds.equals(region.bounds, 1e-6);
        boolean customOSM = files.containsKey("customOpenStreetMapData");

        // Set updatedBy
        region.updatedBy = req.attribute("email");

        if (boundsChanged || customOSM) {
            region.statusCode = Region.StatusCode.STARTED;
            region = Persistence.regions.put(region);

            // Fetch data and update the statuses separately
            fetchOsmAndCensusDataInThread(region._id, files, boundsChanged);
        }

        return region;
    }

    public static Region deleteRegion (Request req, Response res) {
        return Persistence.regions.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));
    }

    public static void register () {
        get("/api/region", RegionController::getAllRegions, JsonUtil.objectMapper::writeValueAsString);
        get("/api/region/:_id", RegionController::getRegion, JsonUtil.objectMapper::writeValueAsString);
        get("/api/region/:region/scenarios", ScenarioController::getAllScenarios, JsonUtil.objectMapper::writeValueAsString);
        get("/api/region/:region/bookmark", BookmarkController::getAllBookmarks, JsonUtil.objectMapper::writeValueAsString);
        post("/api/region/:region/bookmark", BookmarkController::createBookmark, JsonUtil.objectMapper::writeValueAsString);
        post("/api/region", RegionController::create, JsonUtil.objectMapper::writeValueAsString);
        put("/api/region/:_id", RegionController::update, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/region/:_id", RegionController::deleteRegion, JsonUtil.objectMapper::writeValueAsString);
    }
}
