package com.conveyal.taui.models;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.taui.AnalysisServerException;

import java.time.LocalDate;
import java.util.List;

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

    public static String bundleScopeFeedId (String feedId, String bundleId) {
        return String.format("%s_%s", feedId, bundleId);
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

        public FeedSummary(GTFSFeed feed, String bundleId) {
            feedId = feed.feedId;
            bundleScopedFeedId = bundleScopeFeedId(feed.feedId, bundleId);
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
