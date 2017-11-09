package com.conveyal.taui.models;

import com.conveyal.taui.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonView;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a project, which is a set of GTFS, OSM, and land use data for a particular location.
 */
public class Project extends Model implements Cloneable {
    /** Project description */
    public String description;

    /** R5 version used for analysis */
    public String r5Version;

    /** Bounds of this project */
    public Bounds bounds;

    /** Does this project use custom OSM */
    public boolean customOsm;

    /** Load status of this project */
    public StatusCode statusCode;
    public String statusMessage;

    // don't persist to DB but do expose to API
    @JsonView(JsonViews.Api.class)
    public List<Bundle> getBundles () {
        return Persistence.bundles.values()
                .stream()
                .filter(b -> _id.equals(b.projectId))
                .collect(Collectors.toList());
    }

    @JsonView(JsonViews.Api.class)
    public List<Scenario> getScenarios () {
        return Persistence.scenarios.values()
                .stream()
                .filter(s -> _id.equals(s.projectId))
                .collect(Collectors.toList());
    }

    @JsonView(JsonViews.Api.class)
    public Collection<Bookmark> getBookmarks () {
        return Persistence.bookmarks.getByProperty("projectId", _id);
    }

    @JsonView(JsonViews.Api.class)
    public Collection<AggregationArea> getAggregationAreas () {
        return Persistence.aggregationAreas.getByProperty("projectId", _id);
    }

    public List<OpportunityDataset> opportunityDatasets = new ArrayList<>();

    public Project clone () {
        try {
            return (Project) super.clone();
        } catch (CloneNotSupportedException e) {
            // can't happen.
            throw new RuntimeException(e);
        }
    }

    /** Represents an Opportunity Dataset */
    public static class OpportunityDataset {
        /** The human-readable name of the data source from which this came */
        public String dataSource;

        /** The human readable name of the dataset */
        public String name;

        /** The key on S3 */
        public String key;
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
