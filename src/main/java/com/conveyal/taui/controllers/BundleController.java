package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.gtfs.GTFSCache;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.r5.analyst.cluster.BundleManifest;
import com.conveyal.r5.util.ExceptionUtils;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.ExecutorServices;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Envelope;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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

    private static final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withRegion(AnalysisServerConfig.awsRegion)
            .build();

    public static Bundle create (Request req, Response res) {
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);

        // create the bundle
        Map<String, List<FileItem>> files;
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
            bundle.errorCode = ExceptionUtils.asString(e);
            Persistence.bundles.put(bundle);
            bundleFile.delete();

            throw AnalysisServerException.unknown(e);
        }

        // process async
        ExecutorServices.heavy.execute(() -> {
            try {
                Set<String> seenFeedIds = new HashSet<>();

                Envelope bundleBounds = new Envelope();
                bundle.serviceStart = LocalDate.MAX;
                bundle.serviceEnd = LocalDate.MIN;
                bundle.feeds = new ArrayList<>();
                bundle.totalFeeds = localFiles.size();

                for (File file : localFiles) {
                    FeedSource fs = ApiMain.registerFeedSource(feed -> String.format("%s_%s", feed.feedId, bundle._id), file);
                    if (seenFeedIds.contains(fs.feed.feedId)) {
                        throw new Exception("Duplicate Feed ID found when uploading bundle");
                    }

                    Bundle.FeedSummary feedSummary  = new Bundle.FeedSummary(fs.feed, bundle._id);
                    bundle.feeds.add(feedSummary);

                    for (Stop s : fs.feed.stops.values()) {
                        bundleBounds.expandToInclude(s.stop_lon, s.stop_lat);
                    }

                    if (bundle.serviceStart.isAfter(feedSummary.serviceStart)) {
                        bundle.serviceStart = feedSummary.serviceStart;
                    }

                    if (bundle.serviceEnd.isBefore(feedSummary.serviceEnd)) {
                        bundle.serviceEnd = feedSummary.serviceEnd;
                    }

                    bundle.feedsComplete += 1;

                    // Done in a loop the nonce and updatedAt would be changed repeatedly
                    Persistence.bundles.modifiyWithoutUpdatingLock(bundle);
                }

                // TODO Handle crossing the antimeridian
                bundle.north = bundleBounds.getMaxY();
                bundle.south = bundleBounds.getMinY();
                bundle.east = bundleBounds.getMaxX();
                bundle.west = bundleBounds.getMinX();

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
        File cacheDir = new File(AnalysisServerConfig.localCacheDirectory);
        String manifestFileName = GTFSCache.cleanId(bundle._id) + ".json";
        File manifestFile = new File(cacheDir, manifestFileName);
        JsonUtil.objectMapper.writeValue(manifestFile, manifest);

        if (!AnalysisServerConfig.offline) {
            // upload to cache bucket
            s3.putObject(AnalysisServerConfig.bundleBucket, manifestFileName, manifestFile);
        }
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
        return Persistence.bundles.updateFromJSONRequest(req);
    }

    public static Bundle getBundle (Request req, Response res) {
        Bundle bundle = Persistence.bundles.findByIdFromRequestIfPermitted(req);

        // Progressively update older bundles with service start and end dates on retrieval
        try {
            setBundleServiceDates(bundle);
        } catch (Exception e) {
            throw AnalysisServerException.unknown(e);
        }

        return bundle;
    }

    public static Collection<Bundle> getBundles (Request req, Response res) {
        return Persistence.bundles.findPermittedForQuery(req);
    }

    /**
     * Bundles created before 2018-10-04 do not have service start and end dates. This method sets the service start
     * and end dates for bundles that are DONE and do not have them set already. A database migration wasn't done due to
     * the need to load feeds which is a heavy operation. Duplicate functionality exists in the Bundle.FeedSummary
     * constructor and these dates will be automatically set for all new Bundles.
     */
    public static Bundle setBundleServiceDates (Bundle bundle) throws Exception {
        if (bundle.status != Bundle.Status.DONE || (bundle.serviceStart != null && bundle.serviceEnd != null)) return bundle;

        bundle.serviceStart = LocalDate.MAX;
        bundle.serviceEnd = LocalDate.MIN;

        for (Bundle.FeedSummary summary : bundle.feeds) {
            // Compute the feed start and end dates
            if (summary.serviceStart == null || summary.serviceEnd == null) {
                FeedSource fs = ApiMain.getFeedSource(Bundle.bundleScopeFeedId(summary.feedId, bundle._id));
                summary.setServiceDates(fs.feed);
            }

            if (summary.serviceStart.isBefore(bundle.serviceStart)) {
                bundle.serviceStart = summary.serviceStart;
            }
            if (summary.serviceEnd.isAfter(bundle.serviceEnd)) {
                bundle.serviceEnd = summary.serviceEnd;
            }
        }

        // Automated change that could occur on a `get`, so don't update the nonce
        return Persistence.bundles.modifiyWithoutUpdatingLock(bundle);
    }

    public static void register () {
        get("/api/bundle", BundleController::getBundles, JsonUtil.objectMapper::writeValueAsString);
        get("/api/bundle/:_id", BundleController::getBundle, JsonUtil.objectMapper::writeValueAsString);
        post("/api/bundle", BundleController::create, JsonUtil.objectMapper::writeValueAsString);
        put("/api/bundle/:_id", BundleController::update, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/bundle/:_id", BundleController::deleteBundle, JsonUtil.objectMapper::writeValueAsString);
    }

    private static FileItemFactory fileItemFactory = new DiskFileItemFactory();
}
