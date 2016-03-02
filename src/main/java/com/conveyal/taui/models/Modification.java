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
        @JsonSubTypes.Type(name = "remove-stops", value = RemoveStops.class)
})
public abstract class Modification extends Model {
    /** the type of this modification, see JsonSubTypes annotation above */
    public abstract String getType ();
}
