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
        public String id;
        
        /** Days of the week on which this service is active */
        public boolean monday, tuesday, wednesday, thursday, friday, saturday, sunday;

        /** allow naming timetables so it's easier to see what's going on */
        public String name;

        /** Speed, kilometers per hour, for each segment */
        public int[] segmentSpeeds;
        
        /** start time (seconds since GTFS midnight) */
        public int startTime;

        /** end time for frequency-based trips (seconds since GTFS midnight) */
        public int endTime;

        /** Dwell time at each stop, seconds */
        public int dwellTime;

        /** headway for frequency-based patterns */
        public int headwaySecs;

        /** should this be specified as an exact schedule */
        public boolean exactTimes;

        /** Phase at a stop that is in this modification */
        public String phaseAtStop;

        /**
         * Phase from a timetable (frequency entry) on another modification.
         * Syntax is `${modification.id}:${timetable.id}`
         */
        public String phaseFromTimetable;

        /** Phase from a stop that can be found in the phased from modification's stops */
        public String phaseFromStop;

        /** Amount of time to phase from the other lines frequency */
        public int phaseSeconds;
    }
}
