package com.conveyal.taui.models;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.taui.TransportAnalyst;
import com.conveyal.taui.persistence.Persistence;

import java.io.File;
import java.time.LocalDate;
import java.util.List;

/**
 * Represents a transport bundle (GTFS and OSM).
 *
 * All of the data is stored in S3, however some information is cached here.
 */
public class Bundle extends Model {
    public String projectId;

    public String name;

    public double north;
    public double south;
    public double east;
    public double west;

    public LocalDate serviceStart;
    public LocalDate serviceEnd;

    public List<FeedSummary> feeds;

    /** load all bundles into the GTFS API */
    public static void load() {
        Persistence.bundles.values().parallelStream().forEach(b -> {
            File directory = new File(new File(TransportAnalyst.config.getProperty("dataDirectory", "data")), b.id);

            b.feeds.forEach(fs -> {
                ApiMain.feedSources.put(fs.feedId, new FeedSource(new File(directory, fs.fileName).getAbsolutePath()));
            });
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
