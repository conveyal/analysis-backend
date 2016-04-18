package com.conveyal.taui.models;

import com.conveyal.geojson.GeometryDeserializer;
import com.conveyal.geojson.GeometrySerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.vividsolutions.jts.geom.LineString;

import java.util.List;

/**
 * Created by matthewc on 3/28/16.
 */
public class AddStops extends Modification {
    public String name;

    public String feed;
    public String[] routes;
    public String[] trips;

    public String fromStop;
    public String toStop;

    public List<AddTripPattern.Segment> segments;

    /** speed of the adjusted segment, km/h */
    public double speed;

    /** dwell time at adjusted stops, seconds */
    public int dwell;

    @Override
    public String getType() {
        return "add-stops";
    }
}
