package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.google.common.io.Files;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.conveyal.taui.util.SparkUtil.haltWithJson;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Created by matthewc on 3/14/16.
 */
public class BundleController {
    private static final Logger LOG = LoggerFactory.getLogger(BundleController.class);

    private static final AmazonS3 s3 = new AmazonS3Client();

    public static Bundle create (Request req, Response res) throws Exception {
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        Map<String, List<FileItem>> files = sfu.parseParameterMap(req.raw());

        String bundleId = UUID.randomUUID().toString().replace("-", "");

        // cache bundle on disk to avoid OOME
        File bundleFile = File.createTempFile(bundleId, ".zip");

        ObjectMetadata om = new ObjectMetadata();
        om.setContentType("application/zip");

        List<File> localFiles = new ArrayList<>();
        File directory = Files.createTempDir();

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

            File localFile = new File(directory, fname);
            fi.write(localFile);
            localFiles.add(localFile);
        }

        // don't run out of disk space
        bundleFile.delete();

        // create the bundle
        Bundle bundle = new Bundle();
        bundle.id = bundleId;
        bundle.name = files.get("Name").get(0).getString("UTF-8");
        bundle.projectId = files.get("projectId").get(0).getString("UTF-8");

        bundle.status = Bundle.Status.PROCESSING_GTFS;
        bundle.totalFeeds = localFiles.size();

        Persistence.bundles.put(bundleId, bundle);

        // make a protective copy to avoid potential issues with modify objects that are in the process
        // of being saved to mongodb
        final Bundle finalBundle = bundle.clone();

        // process async
        new Thread(() -> {
            TDoubleList lats = new TDoubleArrayList();
            TDoubleList lons = new TDoubleArrayList();

            finalBundle.feeds = new ArrayList<>();

            Map<String, FeedSource> feeds = localFiles.stream()
                    .map(file -> {
                        try {
                            FeedSource fs = ApiMain.registerFeedSource(feed -> String.format("%s_%s", feed.feedId, bundleId), file);
                            finalBundle.feedsComplete += 1;
                            Persistence.bundles.put(finalBundle.id, finalBundle);
                            return fs;
                        } catch (Exception e) {
                            // This catches any error while processing a feed with the GTFS Api and needs to be more
                            // robust in bubbling up the specific errors to the UI. Really, we need to separate out the
                            // idea of bundles, track uploads of single feeds at a time, and allow the creation of a
                            // "bundle" at a later point. This updated error handling is a stopgap until we improve that
                            // flow.
                            finalBundle.status = Bundle.Status.ERROR;
                            finalBundle.errorCode = e.getMessage();
                            Persistence.bundles.put(bundleId, finalBundle);
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toMap(f -> f.id, f -> f));

            Set<String> seenFeedIds = new HashSet<>();

            for (FeedSource fs : feeds.values()) {
                if (seenFeedIds.contains(fs.feed.feedId)) {
                    finalBundle.status = Bundle.Status.ERROR;
                    finalBundle.errorCode = "duplicate-feed-id";
                    Persistence.bundles.put(bundleId, finalBundle);
                    return;
                }

                seenFeedIds.add(fs.feed.feedId);

                finalBundle.feeds.add(new Bundle.FeedSummary(fs.feed, finalBundle));

                fs.feed.stops.values().forEach(s -> {
                    lats.add(s.stop_lat);
                    lons.add(s.stop_lon);
                });
            }

            // find the median stop location
            // use a median because it is robust to outliers
            lats.sort();
            lons.sort();
            // not a true median as we don't handle the case when there is an even number of stops
            // and we are supposed to average the two middle value, but close enough
            finalBundle.centerLat = lats.get(lats.size() / 2);
            finalBundle.centerLon = lons.get(lons.size() / 2);

            try {
                finalBundle.writeManifestToCache();
                finalBundle.status = Bundle.Status.DONE;
            } catch (IOException e) {
                LOG.error("Error writing bundle manifest to cache", e);
                finalBundle.status = Bundle.Status.ERROR;
                finalBundle.errorCode = "cache-write-error";
            }

            Persistence.bundles.put(bundleId, finalBundle);

            directory.delete();
        }).start();

        return bundle;
    }

    public static Bundle deleteBundle (Request req, Response res) {
        Bundle bundle = Persistence.bundles.get(req.params("id"));
        if (bundle == null) {
            haltWithJson(404, "The bundle you are trying to delete does not exist.");
        }

        Persistence.bundles.remove(bundle.id);

        // free memory
        //ApiMain.invalidate(bundle.id);

        if (AnalysisServerConfig.bundleBucket != null) {
            // remove from s3
            s3.deleteObject(AnalysisServerConfig.bundleBucket, bundle.id + ".zip");
        }

        return bundle;
    }

    public static Bundle update (Request req, Response res) {
        Bundle bundle = null;
        try {
            bundle = JsonUtilities.objectMapper.readValue(req.body(), Bundle.class);
        } catch (IOException e) {
            haltWithJson(400, "Unable to parse Bundle JSON from the client.");
        }

        // bundles are mostly immutable, only copy relevant things
        Bundle existing = Persistence.bundles.get(bundle.id);

        if (existing == null) {
            haltWithJson(404, "The bundle you are trying to edit does not exist.");
        }

        existing = existing.clone();
        existing.name = bundle.name;
        existing.feeds = bundle.feeds;

        Persistence.bundles.put(existing.id, existing);
        return bundle;
    }

    public static Object getBundle (Request req, Response res) {
        String id = req.params("id");

        Bundle bundle = Persistence.bundles.get(id);

        if (bundle == null) {
            haltWithJson(404, "Bundle " + id + " was not found. Have you deleted the bundle that was used for this scenario?");
        }
        return bundle;
    }

    public static void register () {
        get("/api/bundle/:id", BundleController::getBundle, JsonUtil.objectMapper::writeValueAsString);
        post("/api/bundle", BundleController::create, JsonUtil.objectMapper::writeValueAsString);
        put("/api/bundle/:id", BundleController::update, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/bundle/:id", BundleController::deleteBundle, JsonUtil.objectMapper::writeValueAsString);
    }

    private static FileItemFactory fileItemFactory = new DiskFileItemFactory();
}
