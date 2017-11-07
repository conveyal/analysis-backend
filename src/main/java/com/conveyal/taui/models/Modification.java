package com.conveyal.taui.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Created by matthewc on 2/9/16.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.PROPERTY, property="type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "add-trip-pattern", value = AddTripPattern.class),
        @JsonSubTypes.Type(name = "set-trip-phasing", value = SetPhasing.class),
        @JsonSubTypes.Type(name = "remove-trips", value = RemoveTrips.class),
        @JsonSubTypes.Type(name = "remove-stops", value = RemoveStops.class),
        @JsonSubTypes.Type(name = "adjust-speed", value = AdjustSpeed.class),
        @JsonSubTypes.Type(name = "adjust-dwell-time", value = AdjustDwellTime.class),
        @JsonSubTypes.Type(name = "convert-to-frequency", value = ConvertToFrequency.class),
        @JsonSubTypes.Type(name = "reroute", value = Reroute.class)
})
public abstract class Modification extends Model implements Cloneable {
    /** the type of this modification, see JsonSubTypes annotation above */
    public abstract String getType ();

    public Modification clone () throws CloneNotSupportedException {
        return (Modification) super.clone();
    }

    /** What scenario is this modification a part of? */
    public String scenarioId;

    /** what variants is this modification a part of? */
    public boolean[] variants;

    /** is this modification shown on the map in the UI at the moment? */
    public boolean showOnMap = true;

    // TODO remove this, it is no longer used
    /** is this modification expanded in the UI at the moment? */
    public boolean expanded = true;

    /** A description/comment about this modification */
    public String description;
}
