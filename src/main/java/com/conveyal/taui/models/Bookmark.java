package com.conveyal.taui.models;

import com.conveyal.r5.profile.ProfileRequest;

import java.time.LocalDate;

/**
 * A bookmark represents "frozen" settings for single point results.
 */
public class Bookmark extends Model {
    /** The name of this bookmark */
    public String name;

    public ProfileRequest profileRequest;

    public int isochroneCutoff;

    /** The destination grid */
    public String destinationGrid;

    /** The project ID of this bookmark */
    public String projectId;
}
