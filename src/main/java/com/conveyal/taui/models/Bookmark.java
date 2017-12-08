package com.conveyal.taui.models;

/**
 * A bookmark represents "frozen" settings for single point results.
 */
public class Bookmark extends Model {
    public AnalysisRequest profileRequest;

    public int isochroneCutoff;

    /** The destination grid */
    public String opportunityDataset;

    /** The region ID of this bookmark */
    public String regionId;
}
