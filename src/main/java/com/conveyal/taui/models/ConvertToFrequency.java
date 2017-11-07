package com.conveyal.taui.models;

import java.util.List;

/**
 * Convert a line to frequency.
 */
public class ConvertToFrequency extends Modification {
    public static final String type = "convert-to-frequency";
    @Override
    public String getType() {
        return "convert-to-frequency";
    }

    public String feed;
    public String[] routes;

    /** Should trips on this route that start outside the days/times specified by frequency entries be retained? */
    public boolean retainTripsOutsideFrequencyEntries = false;

    public List<FrequencyEntry> entries;

    public static class FrequencyEntry extends TimetableInterface {
        /** start times of this trip (seconds since midnight), when non-null scheduled trips will be created */
        @Deprecated
        public int[] startTimes;

        /** trip from which to copy travel times */
        public String sourceTrip;

        /** trips on the selected patterns which could be used as source trips */
        public String[] patternTrips;
    }
}
