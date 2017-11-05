package com.conveyal.taui.models;

/**
 * Created by matthewc on 2/12/16.
 */
public class SetPhasing extends Modification {
    public String getType() {
        return "set-trip-phasing";
    }

    public int phaseSeconds;

    public int sourceStopSequence;

    public int targetStopSequence;

    public String sourceTripId;

    public String targetTripId;
}
