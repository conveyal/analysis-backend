package com.conveyal.taui.models;

import com.amazonaws.partitions.model.Region;
import com.conveyal.r5.common.SphericalDistanceLibrary;
import com.conveyal.r5.profile.ProfileRequest;
import com.conveyal.taui.analysis.RegionalAnalysisManager;
import com.fasterxml.jackson.annotation.JsonView;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import static com.conveyal.r5.analyst.Grid.latToPixel;
import static com.conveyal.r5.analyst.Grid.lonToPixel;

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

    /** Percentile this analysis is using, or -1 if it is pre-percentiles and is using Andrew Owen-style accessibility */
    public int travelTimePercentile = -1;

    public int variant;
    public String projectId;
    public String grid;
    public int cutoffMinutes;

    /**
     * A geometry defining the bounds of this regional analysis.
     * For now, we will use the bounding box of this geometry, but eventually we should figure out which
     * points should be included and only include those points. When you have an irregular analysis region,
     * See also: https://commons.wikimedia.org/wiki/File:The_Gerry-Mander_Edit.png
     */
    public Geometry bounds;

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

    public void computeBoundingBoxFromBounds () {
        Envelope bbox = bounds.getEnvelopeInternal();
        west = lonToPixel(bbox.getMinX(), zoom);
        width = lonToPixel(bbox.getMaxX(), zoom) - west;
        north = latToPixel(bbox.getMaxY(), zoom);
        height = latToPixel(bbox.getMinY(), zoom) - north;
    }

    public void computeBoundingBoxFromProject (Project project) {
        west = lonToPixel(project.bounds.west, zoom);
        width = lonToPixel(project.bounds.east, zoom) - west;
        north = latToPixel(project.bounds.north, zoom);
        height = latToPixel(project.bounds.south, zoom) - north;
    }

    public RegionalAnalysis clone () {
        try {
            return (RegionalAnalysis) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
