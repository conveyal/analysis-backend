package com.conveyal.taui.models;

import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonView;
import com.mongodb.QueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents a region, which is a set of GTFS, OSM, and land use data for a particular location.
 */
public class Region extends Model implements Cloneable {
    /** Region description */
    public String description;

    /** R5 version used for analysis */
    public String r5Version;

    /** Bounds of this region */
    public Bounds bounds;

    /** Does this region use custom OSM */
    public boolean customOsm;

    /** Load status of this region */
    public StatusCode statusCode;
    public String statusMessage;

    @JsonView(JsonViews.Api.class)
    public Collection<Bookmark> getBookmarks () {
        return Persistence.bookmarks.getByProperty("regionId", _id);
    }

    @JsonView(JsonViews.Api.class)
    public Collection<AggregationArea> getAggregationAreas () {
        return Persistence.aggregationAreas.getByProperty("regionId", _id);
    }

    public Region clone () {
        try {
            return (Region) super.clone();
        } catch (CloneNotSupportedException e) {
            // can't happen.
            throw AnalysisServerException.unknown(e);
        }
    }

    /** Represents the status of OSM and Census loading */
    public enum StatusCode {
        /** Started **/
        STARTED,

        /** Downloading OpenStreetMap data */
        DOWNLOADING_OSM,

        /** Downloading Census data using seamless-census */
        DOWNLOADING_CENSUS,

        /** Error while doing any of these operations */
        ERROR,

        /** Done **/
        DONE
    }
}
