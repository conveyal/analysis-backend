package com.conveyal.taui.models;

import com.conveyal.r5.analyst.scenario.AddTrips;
import com.conveyal.taui.AnalysisServerException;

import java.util.ArrayList;
import java.util.List;

/**
 * Add a trip pattern.
 */
public class AddTripPattern extends Modification {
    public static final String type = "add-trip-pattern";
    public String getType() {
        return type;
    }

    // Using Integer because transitMode can be null
    public Integer transitMode;

    public List<Segment> segments;

    public boolean bidirectional;

    public List<Timetable> timetables;

    public static class Timetable extends AbstractTimetable {
        /** Default dwell time, seconds */
        public int dwellTime;

        /** Speed, kilometers per hour, for each segment */
        public int[] segmentSpeeds;

        /** Dwell times at adjusted stops, seconds */
        // using Integer not int because dwell times can be null
        public Integer[] dwellTimes;

        public AddTrips.PatternTimetable toR5 (List<ModificationStop> stops) {
            AddTrips.PatternTimetable pt = this.toBaseR5Timetable();

            // Get hop times
            pt.dwellTimes = ModificationStop.getDwellTimes(stops);
            pt.hopTimes = ModificationStop.getHopTimes(stops);

            return pt;
        }
    }

    public AddTrips toR5 () {
        AddTrips at = new AddTrips();
        at.comment = name;

        if (transitMode != null) {
            at.mode = transitMode;
        }

        at.bidirectional = bidirectional;
        at.frequencies = new ArrayList<>();

        // Iterate over the timetables generating hopTimes and dwellTimes from the segments and segment speeds
        for (int i = 0; i < timetables.size(); i++) {
            Timetable tt = timetables.get(i);
            // Stop distance calculations are repeated but this is a short term fix until the models are updated.
            List<ModificationStop> stops;
            // TODO handle errors converting modifications toR5 more generally.
            try {
                stops = ModificationStop.getStopsFromSegments(segments, tt.dwellTimes, tt.dwellTime, tt.segmentSpeeds);
            } catch (AnalysisServerException ase) {
                throw AnalysisServerException.badRequest("Error in " + name + ": " + ase.message);
            }

            AddTrips.PatternTimetable pt = tt.toR5(stops);
            at.frequencies.add(pt);
        }

        // Values for stop spec are not affected by time table segment speeds or dwell times
        at.stops = ModificationStop.toStopSpecs(ModificationStop.getStopsFromSegments(segments, null, 0, new int[0]));

        return at;
    }
}
