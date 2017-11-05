package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
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

        // create the bundle
        final Bundle bundle = new Bundle();
        bundle.name = files.get("Name").get(0).getString("UTF-8");
        bundle.projectId = files.get("projectId").get(0).getString("UTF-8");

        bundle.status = Bundle.Status.PROCESSING_GTFS;

        // Set `createdBy` and `accessGroup`
        bundle.accessGroup = req.attribute("accessGroup");
        bundle.createdBy = req.attribute("email");

        Persistence.bundles.create(bundle);

        File bundleFile = null;
        File directory = Files.createTempDir();
        List<File> localFiles = new ArrayList<>();
        try {
            // cache bundle on disk to avoid OOME
            bundleFile = File.createTempFile(bundle._id, ".zip");

            ObjectMetadata om = new ObjectMetadata();
            om.setContentType("application/zip");

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
        } catch (Exception e) {
            bundle.status = Bundle.Status.ERROR;
            bundle.errorCode = e.getMessage();
            Persistence.bundles.put(bundle);
            bundleFile.delete();

            throw AnalysisServerException.Unknown(e);
        }

        // process async
        new Thread(() -> {
            TDoubleList lats = new TDoubleArrayList();
            TDoubleList lons = new TDoubleArrayList();

            bundle.feeds = new ArrayList<>();
            bundle.totalFeeds = localFiles.size();

            Map<String, FeedSource> feeds = localFiles.stream()
                    .map(file -> {
                        try {
                            FeedSource fs = ApiMain.registerFeedSource(feed -> String.format("%s_%s", feed.feedId, bundle._id), file);
                            bundle.feedsComplete += 1;
                            Persistence.bundles.put(bundle);
                            return fs;
                        } catch (Exception e) {
                            // This catches any error while processing a feed with the GTFS Api and needs to be more
                            // robust in bubbling up the specific errors to the UI. Really, we need to separate out the
                            // idea of bundles, track uploads of single feeds at a time, and allow the creation of a
                            // "bundle" at a later point. This updated error handling is a stopgap until we improve that
                            // flow.
                            bundle.status = Bundle.Status.ERROR;
                            bundle.errorCode = e.getMessage();
                            Persistence.bundles.put(bundle);
                            throw AnalysisServerException.Unknown(e);
                        }
                    })
                    .collect(Collectors.toMap(f -> f.id, f -> f));

            Set<String> seenFeedIds = new HashSet<>();

            for (FeedSource fs : feeds.values()) {
                if (seenFeedIds.contains(fs.feed.feedId)) {
                    bundle.status = Bundle.Status.ERROR;
                    bundle.errorCode = "duplicate-feed-_id";
                    Persistence.bundles.put(bundle);
                    return;
                }

                seenFeedIds.add(fs.feed.feedId);

                bundle.feeds.add(new Bundle.FeedSummary(fs.feed, bundle));

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
            bundle.centerLat = lats.get(lats.size() / 2);
            bundle.centerLon = lons.get(lons.size() / 2);

            try {
                bundle.writeManifestToCache();
                bundle.status = Bundle.Status.DONE;
            } catch (IOException e) {
                LOG.error("Error writing bundle manifest to cache", e);
                bundle.status = Bundle.Status.ERROR;
                bundle.errorCode = "cache-write-error";
            }

            Persistence.bundles.put(bundle);

            directory.delete();
        }).start();

        return bundle;
    }

    public static Bundle deleteBundle (Request req, Response res) {
        Bundle bundle = Persistence.bundles.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));

        if (AnalysisServerConfig.bundleBucket != null) {
            // remove from s3
            s3.deleteObject(AnalysisServerConfig.bundleBucket, bundle._id + ".zip");
        }

        return bundle;
    }

    public static Bundle update (Request req, Response res) throws IOException {
        return Persistence.bundles.updateFromJSONRequest(req, Bundle.class);
    }

    public static Object getBundle (Request req, Response res) {
        return Persistence.bundles.findByIdFromRequestIfPermitted(req);
    }

    public static void register () {
        get("/api/bundle/:_id", BundleController::getBundle, JsonUtil.objectMapper::writeValueAsString);
        post("/api/bundle", BundleController::create, JsonUtil.objectMapper::writeValueAsString);
        put("/api/bundle/:_id", BundleController::update, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/bundle/:_id", BundleController::deleteBundle, JsonUtil.objectMapper::writeValueAsString);
    }

    private static FileItemFactory fileItemFactory = new DiskFileItemFactory();
}
