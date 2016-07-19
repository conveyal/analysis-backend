package com.conveyal.taui.models;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.gtfs.GTFSCache;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.r5.analyst.cluster.BundleManifest;
import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.TransportAnalyst;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.mashape.unirest.http.Unirest;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * Represents a transport bundle (GTFS and OSM).
 *
 * All of the data is stored in S3, however some information is cached here.
 */
public class Bundle extends Model implements Cloneable {
    public String projectId;

    public String name;

    @Deprecated
    public String group;

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
    public String errorCode;

    private static final AmazonS3 s3 = new AmazonS3Client();

    public void writeManifestToCache () throws IOException {
        BundleManifest manifest = new BundleManifest();
        manifest.osmId = this.projectId;
        manifest.gtfsIds = this.feeds.stream().map(f -> f.id).collect(Collectors.toList());
        File cacheDir = new File(AnalystConfig.localCache);
        String manifestFileName = GTFSCache.cleanId(this.id) + ".json";
        File manifestFile = new File(cacheDir, manifestFileName);
        JsonUtil.objectMapper.writeValue(manifestFile, manifest);

        if (!AnalystConfig.offline) {
            // upload to cache bucket
            s3.putObject(AnalystConfig.bundleBucket, manifestFileName, manifestFile);
        }
    }

    public Bundle clone () {
        try {
            return (Bundle) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class FeedSummary implements Cloneable {
        public String feedId;
        public String name;
        public String originalFileName;
        public String fileName;
        public String id;
        public LocalDate serviceStart;
        public LocalDate serviceEnd;
        public long checksum;

        public FeedSummary(GTFSFeed feed, Bundle bundle) {
            feedId = feed.feedId;
            id = String.format("%s_%s", feed.feedId, bundle.id);
            name = feed.agency.values().iterator().next().agency_name;
            checksum = feed.checksum;
        }

        /** restore default constructor for use in deserialization */
        public FeedSummary () { /* do nothing */ }

        public FeedSummary clone () {
            try {
                return (FeedSummary) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String toString () {
        return "Bundle " + name + " (" + id + ")";
    }

    public enum Status {
        PROCESSING_GTFS,
        PROCESSING_OSM,
        DONE,
        ERROR
    }
}
