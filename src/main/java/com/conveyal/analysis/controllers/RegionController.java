package com.conveyal.analysis.controllers;

import com.conveyal.analysis.models.Bookmark;
import com.conveyal.analysis.models.Region;
import com.conveyal.analysis.persistence.Persistence;
import com.mongodb.QueryBuilder;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;

import static com.conveyal.analysis.util.JsonUtil.toJson;

public class RegionController implements HttpController {

    public RegionController () {
        // NO COMPONENT DEPENDENCIES
        // Eventually persistence will be a component (AnalysisDatabase) instead of static.
    }

    private Region getRegion (Request req, Response res) {
        return Persistence.regions.findByIdFromRequestIfPermitted(req);
    }

    private Collection<Region> getAllRegions (Request req, Response res) {
        return Persistence.regions.findAllForRequest(req);
    }

    /**
     * Create a region
     * @param req body contains region (.json representation)
     * @param res unused
     * @return Region
     */
    private Region create(Request req, Response res) throws Exception {
        return Persistence.regions.createFromJSONRequest(req);
    }

    private Region update(Request req, Response res) throws Exception {
        return Persistence.regions.updateFromJSONRequest(req);
    }

    private Region deleteRegion (Request req, Response res) {
        return Persistence.regions.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));
    }

    private Collection<Bookmark> getAllBookmarks (Request request, Response response) {
        return Persistence.bookmarks.findPermitted(
                QueryBuilder.start("regionId").is(request.params("region")).get(),
                request.attribute("accessGroup")
        );
    }

    private Bookmark createBookmark (Request request, Response response) throws IOException {
        return Persistence.bookmarks.createFromJSONRequest(request);
    }

    @Override
    public void registerEndpoints (spark.Service sparkService) {
        sparkService.get("/api/region", this::getAllRegions, toJson);
        sparkService.get("/api/region/:_id", this::getRegion, toJson);
        sparkService.post("/api/region", this::create, toJson);
        sparkService.put("/api/region/:_id", this::update, toJson);
        sparkService.delete("/api/region/:_id", this::deleteRegion, toJson);
        sparkService.get("/api/region/:region/bookmarks", this::getAllBookmarks, toJson);
        sparkService.post("/api/region/:region/bookmarks", this::createBookmark, toJson);
    }
}
