package com.conveyal.taui.models;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * An aggregation area defines a set of origin points to be averaged together to produce an aggregate accessibility figure.
 * It is defined by a geometry that is rasterized and stored as a grid, with pixels with values between 0 and 100,000
 * depending on how much of that pixel is overlapped by the mask.
 */
public class AggregationArea extends Model {
    public String projectId;
    public String name;

    @JsonIgnore
    public String getS3Key () {
        return String.format("%s/mask/%s.grid", projectId, _id);
    }
}
