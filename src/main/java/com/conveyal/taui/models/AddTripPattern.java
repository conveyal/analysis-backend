package com.conveyal.taui.models;

import com.conveyal.geojson.GeometryDeserializer;
import com.conveyal.geojson.GeometrySerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.vividsolutions.jts.geom.Geometry;

import java.util.List;

/**
 * Add a trip pattern.
 */
public class AddTripPattern extends Modification {
    public static final String type = "add-trip-pattern";
    public String getType() {
        return type;
    }

    public List<Segment> segments;

    public boolean bidirectional;

    public List<Timetable> timetables;

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

    public static class Timetable extends TimetableInterface {
        /** Dwell time at each stop, seconds */
        public int dwellTime;

        /** Speed, kilometers per hour, for each segment */
        public int[] segmentSpeeds;

        /** Dwell times at specific stops, seconds */
        // using Integer not int because dwell times can be null
        public Integer[] dwellTimes;
    }
}
