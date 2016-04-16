package com.conveyal.taui.models;

import java.util.BitSet;
import java.util.List;

import com.conveyal.geojson.GeometryDeserializer;
import com.conveyal.geojson.GeometrySerializer;
import com.conveyal.r5.model.json_serialization.BitSetDeserializer;
import com.conveyal.r5.model.json_serialization.BitSetSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;

/**
 * Add a trip pattern.
 */
public class AddTripPattern extends Modification {
    public String name;

    public List<Segment> segments;

    public boolean bidirectional;

    public List<Timetable> timetables;

    public String getType() {
        return "add-trip-pattern";
    }

    /** represents a single segment of an added trip pattern (between two user-specified points) */
    public static class Segment {
        /** Is there a stop at the start of this segment */
        public boolean stopAtStart;

        /** Is there a stop at the end of this segment */
        public boolean stopAtEnd;

        /** If this segment starts at an existing stop, what is its feed-scoped stop ID? */
        public String fromStopId;

        /** If this segment ends at an existing stop, what is its feed-scoped stop ID? */
        public String toStopId;

        /** spacing between stops in this segment, meters */
        public int spacing;

        /**
         * Geometry of this segment
         * Generally speaking, this will be a LineString, but the first segment may be a Point
         * iff there are no more segments. This is used when someone first starts drawing a line and
         * they have only drawn one stop so far. Of course a transit line with only one stop would
         * not be particularly useful.
         */
        @JsonDeserialize(using= GeometryDeserializer.class)
        @JsonSerialize(using= GeometrySerializer.class)
        public Geometry geometry;
    }

    public static class Timetable {
        /** Days of the week on which this service is active */
        public boolean monday, tuesday, wednesday, thursday, friday, saturday, sunday;

        /** allow naming timetables so it's easier to see what's going on */
        public String name;

        /** Speed, kilometers per hour */
        public int speed;

        /** hop times in seconds */
        public int[] hopTimes;

        /** dwell times in seconds */
        public int[] dwellTimes;

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
