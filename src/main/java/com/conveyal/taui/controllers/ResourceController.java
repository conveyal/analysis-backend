package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.models.Resource;
import com.conveyal.taui.persistence.Persistence;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.path;
import static spark.Spark.post;
import static spark.Spark.put;

public class ResourceController {
    // Local folder in offline mode and S3 bucket in online mode
    private static final String basePath = "cache";

    private static final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withRegion(AnalysisServerConfig.awsRegion)
            .build();
    private static FileItemFactory fileItemFactory = new DiskFileItemFactory();

    public ResourceController() {
        path("/api/resources", () -> {
            get("", (req, res) -> Persistence.resources.findAllForRequest(req));
            post("", (req, res) -> this.createResource(req, res));
            post("/:_id/upload", (req, res) -> this.uploadResource(req, res));
            get("/:_id", (req, res) -> Persistence.resources.findByIdFromRequestIfPermitted(req));
            get("/:id/download", (req, res) -> this.download(req, res));
            put("/:_id", (req, res) -> Persistence.resources.updateFromJSONRequest(req));
            delete("/:_id", (req, res) -> this.deleteResource(req, res));
        });
    }

    public JSONObject createResource (Request req, Response res) throws IOException {
        Resource resource = Persistence.resources.createFromJSONRequest(req);
        JSONObject data = new JSONObject();
        data.put("resource", resource);

        // Generate an upload URL setup to use S3
        if (!AnalysisServerConfig.offline) {
            data.put("uploadUrl", resource.getUploadURL(s3, basePath));
        }

        return data;
    }

    /**
     * Delete from the database and from S3
     */
    public Object deleteResource (Request req, Response res) {
        Resource resource = Persistence.resources.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));

        if (AnalysisServerConfig.offline) {
            File file = resource.getLocalFile(basePath);
            return file.delete();
        } else {
            s3.deleteObject(basePath, resource.getPath());
        }

        return true;
    }

    public Object download (Request req, Response res) throws Exception {
        Resource resource = Persistence.resources.findByIdFromRequestIfPermitted(req);

        if (AnalysisServerConfig.offline) {
            OutputStream os = res.raw().getOutputStream();
            FileUtils.copyFile(resource.getLocalFile(basePath), os);
            return res.raw();
        } else {
            res.type("text");
            return resource.getDownloadURL(s3, basePath);
        }
    }

    /**
     * Only run in OFFLINE mode
     */
    public boolean uploadResource (Request req, Response res) throws Exception {
        Resource resource = Persistence.resources.findByIdFromRequestIfPermitted(req);

        InputStream is = req.raw().getPart("file").getInputStream();
        FileUtils.copyInputStreamToFile(is, resource.getLocalFile(basePath));

        return true;
    }
}
