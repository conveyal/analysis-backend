package com.conveyal.taui.models;

import java.util.List;

/**
 * Created by matthewc on 3/28/16.
 */
public class Reroute extends Modification {
    public String getType() {
        return "reroute";
    }

    public String feed;
    public String[] routes;
    public String[] trips;

    public String fromStop;
    public String toStop;

    public List<AddTripPattern.Segment> segments;

    /** speed of the adjusted segment, km/h, per segment */
    public int[] segmentSpeeds;

    /** dwell time at adjusted stops, seconds */
    public int dwellTime;

    // using Integer not int because Integers can be null
    public Integer[] dwellTimes;
}
