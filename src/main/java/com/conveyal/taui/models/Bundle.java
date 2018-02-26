package com.conveyal.taui.models;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.conveyal.gtfs.GTFSCache;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.r5.analyst.cluster.BundleManifest;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.util.JsonUtil;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a transport bundle (GTFS and OSM).
 *
 * All of the data is stored in S3, however some information is cached here.
 */
public class Bundle extends Model implements Cloneable {
    public String regionId;

    public double north;
    public double south;
    public double east;
    public double west;

    public double centerLat;
    public double centerLon;

    public LocalDate serviceStart;
    public LocalDate serviceEnd;

    public List<FeedSummary> feeds;
    public Status status;

    public int feedsComplete;
    public int totalFeeds;

    public String errorCode;

    private static final AmazonS3 s3 = new AmazonS3Client();

    public void writeManifestToCache () throws IOException {
        BundleManifest manifest = new BundleManifest();
        manifest.osmId = this.regionId;
        manifest.gtfsIds = this.feeds.stream().map(f -> f.bundleScopedFeedId).collect(Collectors.toList());
        File cacheDir = new File(AnalysisServerConfig.localCacheDirectory);
        String manifestFileName = GTFSCache.cleanId(this._id) + ".json";
        File manifestFile = new File(cacheDir, manifestFileName);
        JsonUtil.objectMapper.writeValue(manifestFile, manifest);

        if (!AnalysisServerConfig.offline) {
            // upload to cache bucket
            s3.putObject(AnalysisServerConfig.bundleBucket, manifestFileName, manifestFile);
        }
    }

    public Bundle clone () {
        try {
            return (Bundle) super.clone();
        } catch (CloneNotSupportedException e) {
            throw AnalysisServerException.unknown(e);
        }
    }

    public static class FeedSummary implements Cloneable {
        public String feedId;
        public String name;
        public String originalFileName;
        public String fileName;

        /** The feed ID scoped with the bundle ID, for use as a unique identifier on S3 and in the GTFS API */
        public String bundleScopedFeedId;

        public LocalDate serviceStart;
        public LocalDate serviceEnd;
        public long checksum;

        public FeedSummary(GTFSFeed feed, Bundle bundle) {
            feedId = feed.feedId;
            bundleScopedFeedId = String.format("%s_%s", feed.feedId, bundle._id);
            name = feed.agency.size() > 0 ? feed.agency.values().iterator().next().agency_name : feed.feedId;
            checksum = feed.checksum;
        }

        /** restore default constructor for use in deserialization */
        public FeedSummary () { /* do nothing */ }

        public FeedSummary clone () {
            try {
                return (FeedSummary) super.clone();
            } catch (CloneNotSupportedException e) {
                throw AnalysisServerException.unknown(e);
            }
        }
    }

    public String toString () {
        return "Bundle " + name + " (" + _id + ")";
    }

    public enum Status {
        PROCESSING_GTFS,
        PROCESSING_OSM,
        DONE,
        ERROR
    }
}
