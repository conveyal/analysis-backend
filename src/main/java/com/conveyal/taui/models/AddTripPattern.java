package com.conveyal.taui.models;

import java.util.BitSet;
import java.util.List;

import com.conveyal.geojson.GeometryDeserializer;
import com.conveyal.geojson.GeometrySerializer;
import com.conveyal.r5.model.json_serialization.BitSetDeserializer;
import com.conveyal.r5.model.json_serialization.BitSetSerializer;
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

    public boolean bidirectional;

    public List<Timetable> timetables;

    public String getType() {
        return "add-trip-pattern";
    }

    public static class Timetable {
        /** Days of the week on which this service is active, 0 is Monday */
        @JsonDeserialize(using = BitSetDeserializer.class)
        @JsonSerialize(using = BitSetSerializer.class)
        public BitSet days;

        /** Speed, kilometers per hour */
        public int speed;

        /** hop times in seconds */
        public int[] hopTimes;

        /** dwell times in seconds */
        public int[] dwellTimes;

        /** is this a frequency entry? */
        public boolean frequency;

        /** start time (seconds since GTFS midnight) */
        public int startTime;

        /** end time for frequency-based trips (seconds since GTFS midnight) */
        public int endTime;

        /** Dwell time at each stop, seconds */
        public int dwellTime;

        /** headway for frequency-based patterns */
        public int headwaySecs;
    }
}
