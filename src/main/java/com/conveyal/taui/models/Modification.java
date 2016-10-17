package com.conveyal.taui.models;

import com.conveyal.r5.model.json_serialization.BitSetDeserializer;
import com.conveyal.r5.model.json_serialization.BitSetSerializer;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.BitSet;

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
public abstract class Modification extends Model {
    /** the type of this modification, see JsonSubTypes annotation above */
    public abstract String getType ();

    /** is this modification shown on the map in the UI at the moment? */
    public boolean showOnMap = true;

    /** is this modification expanded in the UI at the moment? */
    public boolean expanded = true;

    /** what variants is this modification a part of? */
    public boolean[] variants;

    /** What scenario is this modification a part of? */
    public String scenario;
}
