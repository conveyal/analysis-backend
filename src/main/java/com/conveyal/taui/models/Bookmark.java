package com.conveyal.taui.models;

/**
 * A bookmark represents "frozen" settings for single point results.
 */
public class Bookmark extends Model {
    /** The region ID of this bookmark */
    public String regionId;

    public AnalysisRequest profileRequest;

    public int isochroneCutoff;

    /** The destination grid */
    public String opportunityDataset;
}
