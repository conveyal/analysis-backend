package com.conveyal.taui.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by matthewc on 2/9/16.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.PROPERTY, property="type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "add-trip-pattern", value = AddTripPattern.class),
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

    /** What project is this modification a part of? */
    public String projectId;

    /** what variants is this modification a part of? */
    public boolean[] variants;

    /** is this modification shown on the map in the UI at the moment? */
    public boolean showOnMap = true;

    /** A description/comment about this modification */
    public String description;

    public Set<String> feedScopeIds (String feed, String[] ids) {
        if (ids == null) return new HashSet<>();
        return Arrays.stream(ids).map(id -> feedScopeId(feed, id)).collect(Collectors.toSet());
    }

    public String feedScopeId (String feed, String id) {
        return String.format("%s:%s", feed, id);
    }

    public abstract com.conveyal.r5.analyst.scenario.Modification toR5 ();
}
