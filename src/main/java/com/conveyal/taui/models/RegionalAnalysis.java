package com.conveyal.taui.models;

import com.conveyal.r5.analyst.broker.JobStatus;
import com.conveyal.r5.profile.ProfileRequest;
import com.conveyal.taui.controllers.RegionalAnalysisManager;
import com.fasterxml.jackson.annotation.JsonView;

import java.util.Date;

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

    @JsonView(JsonViews.Api.class)
    public RegionalAnalysisManager.RegionalAnalysisStatus getStatus () {
        return RegionalAnalysisManager.getStatus(this.id);
    }
}
