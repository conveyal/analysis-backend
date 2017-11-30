package com.conveyal.taui.models;

import com.conveyal.r5.profile.ProfileRequest;

/**
 * A bookmark represents "frozen" settings for single point results.
 */
public class Bookmark extends Model {
    public ProfileRequest profileRequest;

    public int isochroneCutoff;

    /** The destination grid */
    public String opportunityDataset;

    /** The region ID of this bookmark */
    public String regionId;
}
