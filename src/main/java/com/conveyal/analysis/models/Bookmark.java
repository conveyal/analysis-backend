package com.conveyal.analysis.models;

import org.bson.Document;

/**
 * A bookmark represents "frozen" settings for single point results.
 */
public class Bookmark extends Model {
    /** The region ID of this bookmark */
    public String regionId;

    /**
     * Usually corresponds to an AnalysisRequest object, but that definition changes over time. Because this is only
     * used to populate the form on the front-end, we can use an untyped BSON Document that represents any key=value
     * pairs the can be used in MongoDB.
     */
    public Document profileRequest;

    public int isochroneCutoff;

    /** The destination grid */
    public String opportunityDataset; // TODO update bookmarks
}
