package com.conveyal.taui.controllers;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.taui.TransportAnalyst;
import com.conveyal.taui.models.Bundle;
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
import java.util.*;

import static spark.Spark.get;
import static spark.Spark.halt;
import static spark.Spark.post;

/**
 * Created by matthewc on 3/14/16.
 */
public class BundleController {
    private static final Logger LOG = LoggerFactory.getLogger(BundleController.class);

    public static Bundle create (Request req, Response res) throws Exception {
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        Map<String, List<FileItem>> files = sfu.parseParameterMap(req.raw());

        String bundleId = UUID.randomUUID().toString().replace("-", "");

        // create a directory for the bundle
        File directory = new File(TransportAnalyst.config.getProperty("dataDirectory", "data"));
        directory = new File(directory, bundleId);
        directory.mkdirs();

        List<File> localFiles = new ArrayList<>();

        Set<String> usedFileNames = new HashSet<>();

        for (FileItem fi : files.get("files")) {
            // create a unique, safe file name
            String baseName = fi.getName().replace(".zip", "").replaceAll("[^a-zA-Z0-9]", "-");
            String fname = baseName;

            int i = 0;
            while (usedFileNames.contains(fname)) {
                fname = String.format(Locale.US, "%s_%s", fname, i++);

                if (i > 100) {
                    fname = UUID.randomUUID().toString();
                    break;
                }
            }

            usedFileNames.add(fname);

            fname += ".zip";

            File file = new File(directory, fname);
            fi.write(file);
            localFiles.add(file);
        }

        // create the bundle
        Bundle bundle = new Bundle();
        bundle.feeds = new ArrayList<>();
        bundle.id = bundleId;
        bundle.name = files.get("name").get(0).getString("UTF-8");
        bundle.projectId = files.get("projectId").get(0).getString("UTF-8");

        // TODO process asynchronously
        for (File file : localFiles) {
            GTFSFeed feed = GTFSFeed.fromFile(file.getAbsolutePath());

            if (feed.agency.isEmpty()) {
                // TODO really should log original input file name
                LOG.warn("File {} is not a GTFS file", file);
                file.delete();
            }

            Bundle.FeedSummary fs = new Bundle.FeedSummary(feed);
            fs.fileName = file.getName();
            bundle.feeds.add(fs);

            ApiMain.feedSources.put(feed.feedId, new FeedSource(feed));
        }

        Persistence.bundles.put(bundleId, bundle);

        return bundle;
    }

    public static Object getBundles (Request req, Response res) {
        if (req.params("id") != null) {
            String id = req.params("id");

            if (Persistence.bundles.containsKey(id)) return Persistence.bundles.get(id);
            else halt(404);
        }
        else {
            return Persistence.bundles.values();
        }

        return null;
    }

    public static void register () {
        get("/bundle/:id", BundleController::getBundles, JsonUtil.objectMapper::writeValueAsString);
        get("/bundle", BundleController::getBundles, JsonUtil.objectMapper::writeValueAsString);
        post("/bundle", BundleController::create, JsonUtil.objectMapper::writeValueAsString);
    }

    private static FileItemFactory fileItemFactory = new DiskFileItemFactory();
}
