package com.conveyal.taui.models;

import com.conveyal.r5.model.json_serialization.BitSetDeserializer;
import com.conveyal.r5.model.json_serialization.BitSetSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.BitSet;

/**
 * Convert a line to frequency.
 */
public class ConvertToFrequency extends Modification {
    public String name;

    public String feed;
    public String[] routes;

    public FrequencyEntry[] entries;

    @Override
    public String getType() {
        return "convert-to-frequency";
    }

    public static class FrequencyEntry {
        /** Days of the week on which this service is active, 0 is Monday */
        @JsonDeserialize(using = BitSetDeserializer.class)
        @JsonSerialize(using = BitSetSerializer.class)
        public BitSet days;

        /** start time (seconds since GTFS midnight) */
        public int startTime;

        /** end time for frequency-based trips (seconds since GTFS midnight) */
        public int endTime;

        /** headway for frequency-based patterns */
        public int headwaySecs;

        /** trip from which to copy travel times */
        public String sourceTrip;

        /** trips on the selected patterns which could be used as source trips */
        public String[] patternTrips;
    }
}
