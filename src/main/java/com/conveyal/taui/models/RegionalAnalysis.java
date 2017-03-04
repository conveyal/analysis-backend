package com.conveyal.taui.models;

import com.amazonaws.partitions.model.Region;
import com.conveyal.r5.profile.ProfileRequest;
import com.conveyal.taui.analysis.RegionalAnalysisManager;
import com.fasterxml.jackson.annotation.JsonView;

/**
 * Represents a query.
 */
public class RegionalAnalysis extends Model implements Cloneable {
    public int zoom;
    public int width;
    public int height;
    public int north;
    public int west;
    public ProfileRequest request;
    public boolean complete;
    public String name;
    public String workerVersion;
    public long creationTime;

    public String bundleId;

    /** Scenario ID, null if the bundle was used directly */
    public String scenarioId;

    public int variant;
    public String projectId;
    public String grid;
    public int cutoffMinutes;

    // The key of a grid to use as a origins; only cells within this grid will be computed
    public String origins;

    /** Has this analysis been (soft) deleted? */
    public boolean deleted;

    @JsonView(JsonViews.Api.class)
    public RegionalAnalysisManager.RegionalAnalysisStatus getStatus () {
        return RegionalAnalysisManager.getStatus(this.id);
    }

    /**
     * Using JsonViews doesn't work in the database currently due to https://github.com/mongojack/mongojack/issues/145,
     * so the status is saved in the db if anything about the regional analysis is changed.
     */
    public void setStatus (RegionalAnalysisManager.RegionalAnalysisStatus status) {
        // status is not intended to be persisted, ignore it.
    }

    public RegionalAnalysis clone () {
        try {
            return (RegionalAnalysis) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
