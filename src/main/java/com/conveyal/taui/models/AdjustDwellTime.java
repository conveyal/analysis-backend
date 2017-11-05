package com.conveyal.taui.models;

/**
 * Created by matthewc on 3/3/16.
 */
public class AdjustDwellTime extends Modification {
    @Override
    public String getType() {
        return "adjust-dwell-time";
    }

    public String feed;

    public String[] routes;

    public String[] trips;

    public String[] stops;

    /** are we scaling existing times (true) or replacing them with a brand new time (false) */
    public boolean scale;

    /** the factor by which to scale, OR the new time, depending on the value of above */
    public double value;
}
