package com.conveyal.taui.controllers;

import com.conveyal.gtfs.GTFSCache;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.r5.analyst.cluster.BundleManifest;
import com.conveyal.r5.util.ExceptionUtils;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.ExecutorServices;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.persistence.FilePersistence;
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
import java.util.*;
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

    public static Bundle create (Request req, Response res) {
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);

        // create the bundle
        Map<String, List<FileItem>> files = null;
        final Bundle bundle = new Bundle();
        try {
            files = sfu.parseParameterMap(req.raw());

            bundle.name = files.get("Name").get(0).getString("UTF-8");
            bundle.regionId = files.get("regionId").get(0).getString("UTF-8");
        } catch (Exception e) {
            throw AnalysisServerException.badRequest(ExceptionUtils.asString(e));
        }

        bundle.status = Bundle.Status.PROCESSING_GTFS;

        // Set `createdBy` and `accessGroup`
        bundle.accessGroup = req.attribute("accessGroup");
        bundle.createdBy = req.attribute("email");

        Persistence.bundles.create(bundle);

        File directory = Files.createTempDir();
        List<File> localFiles = new ArrayList<>();
        try {
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
        } catch (Exception e) {
            bundle.status = Bundle.Status.ERROR;
            bundle.errorCode = ExceptionUtils.asString(e);
            Persistence.bundles.put(bundle);

            throw AnalysisServerException.unknown(e);
        }

        // process async
        ExecutorServices.heavy.execute(() -> {
            try {
                TDoubleList lats = new TDoubleArrayList();
                TDoubleList lons = new TDoubleArrayList();
                Set<String> seenFeedIds = new HashSet<>();

                bundle.feeds = new ArrayList<>();
                bundle.totalFeeds = localFiles.size();


                for (File file : localFiles) {
                    FeedSource fs = ApiMain.registerFeedSource(feed -> String.format("%s_%s", feed.feedId, bundle._id), file);
                    if (seenFeedIds.contains(fs.feed.feedId)) {
                        throw new Exception("Duplicate Feed ID found when uploading bundle");
                    }

                    bundle.feeds.add(new Bundle.FeedSummary(fs.feed, bundle._id));
                    fs.feed.stops.values().forEach(s -> {
                        lats.add(s.stop_lat);
                        lons.add(s.stop_lon);
                    });

                    bundle.feedsComplete += 1;
                    Persistence.bundles.put(bundle);
                }

                // find the median stop location
                // use a median because it is robust to outliers
                lats.sort();
                lons.sort();
                // not a true median as we don't handle the case when there is an even number of stops
                // and we are supposed to average the two middle value, but close enough
                bundle.centerLat = lats.get(lats.size() / 2);
                bundle.centerLon = lons.get(lons.size() / 2);

                writeManifestToCache(bundle);
                bundle.status = Bundle.Status.DONE;
            } catch (Exception e) {
                // This catches any error while processing a feed with the GTFS Api and needs to be more
                // robust in bubbling up the specific errors to the UI. Really, we need to separate out the
                // idea of bundles, track uploads of single feeds at a time, and allow the creation of a
                // "bundle" at a later point. This updated error handling is a stopgap until we improve that
                // flow.
                LOG.error("Error creating bundle", e);
                bundle.status = Bundle.Status.ERROR;
                bundle.errorCode = ExceptionUtils.asString(e);
            }

            Persistence.bundles.put(bundle);
            directory.delete();
        });

        return bundle;
    }

    private static void writeManifestToCache (Bundle bundle) throws IOException {
        BundleManifest manifest = new BundleManifest();
        manifest.osmId = bundle.regionId;
        manifest.gtfsIds = bundle.feeds.stream().map(f -> f.bundleScopedFeedId).collect(Collectors.toList());
        String manifestFileName = GTFSCache.cleanId(bundle._id) + ".json";

        FilePersistence.in.put(AnalysisServerConfig.bundleBucket, manifestFileName, manifest);
    }

    public static Bundle deleteBundle (Request req, Response res) {
        Bundle bundle = Persistence.bundles.removeIfPermitted(req.params("_id"), req.attribute("accessGroup"));
        FilePersistence.in.delete(AnalysisServerConfig.bundleBucket, String.format("%s.zip", bundle._id));

        return bundle;
    }

    public static Bundle update (Request req, Response res) throws IOException {
        return Persistence.bundles.updateFromJSONRequest(req);
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
