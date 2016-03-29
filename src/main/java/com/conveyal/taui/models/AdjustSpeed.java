package com.conveyal.taui.models;

/**
 * Adjust the speed of a route.
 */
public class AdjustSpeed extends Modification {
    public String name;

    public String feed;

    public String[] routes;

    public String[] trips;

    /** array of [from stop, to stop] specifying single hops this should be applied to */
    public String[][] hops;

    /** the factor by which to scale speed. 1 means no change, 2 means faster. */
    public double scale;

    @Override
    public String getType() {
        return "adjust-speed";
    }
}
