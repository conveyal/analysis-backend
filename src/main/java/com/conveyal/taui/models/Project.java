package com.conveyal.taui.models;

/**
 * Represents a project, which is a set of GTFS, OSM, and land use data for a particular location.
 */
public class Project extends Model {
    /** Project name */
    public String name;

    /** Project description */
    public String description;

    /** R5 version used for analysis */
    public String r5Version;

    /** Bounds of this project */
    public Bounds bounds;

    /** Group this project is associated with */
    public String group;

    public static class Bounds {
        public double north, east, south, west;
    }
}
