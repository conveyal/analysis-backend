package com.conveyal.taui.models;

import java.util.BitSet;
import java.util.List;

import com.conveyal.geojson.GeometryDeserializer;
import com.conveyal.geojson.GeometrySerializer;
import com.conveyal.r5.analyst.scenario.AddTripPattern.PatternTimetable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.vividsolutions.jts.geom.LineString;

/**
 * Add a trip pattern.
 */
public class AddTripPattern extends Modification {
    public String name;

    @JsonDeserialize(using= GeometryDeserializer.class)
    @JsonSerialize(using= GeometrySerializer.class)
    public LineString geometry;
    // in theory these would be more efficient as bitsets but they'd get turned into
    // boolean[] when sent to mongo
    public boolean[] stops;
    public boolean[] controlPoints;

    /** for now just using R5 timetables directly */
    public List<PatternTimetable> timetables;

    public String getType() {
        return "add-trip-pattern";
    }
}
