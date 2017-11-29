package com.conveyal.taui.models;

import com.conveyal.r5.analyst.scenario.AddTrips;

import java.util.List;
import java.util.stream.Collectors;

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

    public static class Timetable extends AbstractTimetable {
        /** Dwell time at each stop, seconds */
        public int dwellTime;

        /** Speed, kilometers per hour, for each segment */
        public int[] segmentSpeeds;

        /** Dwell times at specific stops, seconds */
        // using Integer not int because dwell times can be null
        public Integer[] dwellTimes;

        public AddTrips.PatternTimetable toR5 (List<ModificationStop> stops) {
            AddTrips.PatternTimetable pt = this.toBaseR5Timetable();

            // Get hop times
            pt.dwellTimes = ModificationStop.getDwellTimes(stops, this.dwellTimes, dwellTime);
            pt.hopTimes = ModificationStop.getHopTimes(stops, this.segmentSpeeds);

            return pt;
        }
    }

    public AddTrips toR5 () {
        AddTrips at = new AddTrips();
        at.comment = name;

        at.bidirectional = bidirectional;
        List<ModificationStop> stops = ModificationStop.getStopsFromSegments(segments);
        at.frequencies = timetables.stream().map(tt -> tt.toR5(stops)).collect(Collectors.toList());
        at.stops = ModificationStop.toSpec(stops);

        return at;
    }
}
