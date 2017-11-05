package com.conveyal.taui.models;

/**
 * Convert a line to frequency.
 */
public class ConvertToFrequency extends Modification {
    @Override
    public String getType() {
        return "convert-to-frequency";
    }

    public String feed;
    public String[] routes;

    /** Should trips on this route that start outside the days/times specified by frequency entries be retained? */
    public boolean retainTripsOutsideFrequencyEntries = false;

    public FrequencyEntry[] entries;

    public static class FrequencyEntry {
        public String entryId;

        /** Days of the week on which this service is active, 0 is Monday */
        public boolean monday, tuesday, wednesday, thursday, friday, saturday, sunday;

        /** allow naming entries for organizational purposes */
        public String name;

        /** start time (seconds since GTFS midnight) */
        public int startTime;

        /** end time for frequency-based trips (seconds since GTFS midnight) */
        public int endTime;

        /** headway for frequency-based patterns */
        public int headwaySecs;

        /** Should this frequency entry use exact times? */
        public boolean exactTimes;

        /** Phase at a stop that is in this modification */
        public String phaseAtStop;

        /**
         * Phase from a timetable (frequency entry) on another modification.
         * Syntax is `${modification._id}:${timetable._id}`
         */
        public String phaseFromTimetable;

        /** Phase from a stop that can be found in the phased from modification's stops */
        public String phaseFromStop;

        /** Amount of time to phase from the other lines frequency */
        public int phaseSeconds;

        /** start times of this trip (seconds since midnight), when non-null scheduled trips will be created */
        @Deprecated
        public int[] startTimes;

        /** trip from which to copy travel times */
        public String sourceTrip;

        /** trips on the selected patterns which could be used as source trips */
        public String[] patternTrips;
    }
}
