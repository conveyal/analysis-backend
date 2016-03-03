package com.conveyal.taui.models;

/**
 * Adjust the speed of a route.
 */
public class AdjustSpeed extends Modification {
    public String name;

    public String feed;

    public String[] routes;

    public String[] trips;

    /** are we scaling existing speeds (true) or replacing them with a brand new speed (false) */
    public boolean scale;

    /** the factor by which to scale, OR the new speed, depending on the value of above */
    public double value;

    @Override
    public String getType() {
        return "adjust-speed";
    }
}
