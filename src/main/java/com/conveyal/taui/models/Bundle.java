package com.conveyal.taui.models;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.TransportAnalyst;
import com.conveyal.taui.persistence.Persistence;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * Represents a transport bundle (GTFS and OSM).
 *
 * All of the data is stored in S3, however some information is cached here.
 */
public class Bundle extends Model {
    public String projectId;

    public String name;

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

    /** load all bundles into the GTFS API */
    public static void load() throws IOException {
        AmazonS3 s3 = new AmazonS3Client();
        Persistence.bundles.values().parallelStream().forEach(b -> {
            try {
                File directory = Files.createTempDir();
                File bundleSource = File.createTempFile(b.id, ".zip");

                // transfer from S3
                s3.getObject(new GetObjectRequest(AnalystConfig.bundleBucket, b.id + ".zip"), bundleSource);

                // extract the bundle
                ZipFile bundle = new ZipFile(bundleSource);

                b.feeds.forEach(fs -> {
                    // extract
                    ZipEntry ze = bundle.getEntry(fs.fileName);
                    File file = new File(directory, fs.fileName);

                    try {
                        InputStream in = bundle.getInputStream(ze);
                        OutputStream out = new BufferedOutputStream(new FileOutputStream(file));

                        ByteStreams.copy(in, out);
                        in.close();
                        out.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    ApiMain.feedSources.put(fs.feedId, new FeedSource(file.getAbsolutePath()));

                    file.delete(); // we're done with it now, everything interesting is in memory
                });

                bundleSource.delete();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static class FeedSummary {
        public String feedId;
        public String name;
        public String originalFileName;
        public String fileName;
        public LocalDate serviceStart;
        public LocalDate serviceEnd;

        public FeedSummary(GTFSFeed feed) {
            feedId = feed.feedId;
            name = feed.agency.values().iterator().next().agency_name;
        }

        /** restore default constructor for use in deserialization */
        public FeedSummary () { /* do nothing */ }
    }

    public enum Status {
        PROCESSING_GTFS,
        PROCESSING_OSM,
        DONE,
        ERROR
    }
}
