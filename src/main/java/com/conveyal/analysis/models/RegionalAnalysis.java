package com.conveyal.analysis.models;

import com.conveyal.analysis.AnalysisServerException;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import org.locationtech.jts.geom.Geometry;

/**
 * Represents a single regional (multi-origin) accessibility analysis,
 * which may have more than one percentile and cutoff.
 */
public class RegionalAnalysis extends Model implements Cloneable {
    public String regionId;
    public String bundleId;
    public String projectId;

    public int variant;

    public String workerVersion;

    public int zoom;
    public int width;
    public int height;
    public int north;
    public int west;
    public RegionalTask request;

    /**
     * Percentile of travel time being used in this analysis. Older analyses (pre-X) could have only one percentile.
     * If the analysis is pre-percentiles and is using Andrew Owen-style accessibility, value is -1.
     */
    public int travelTimePercentile = -1;

    /**
     * Newer regional analyses (since release X in February 2020) can have more than one percentile.
     * If this is non-null it completely supersedes travelTimePercentile, which should be ignored.
     */
    public int[] travelTimePercentiles;

    public String grid;

    /** Older analyses (up to about January 2020, before release X) had only one cutoff. */
    public int cutoffMinutes;

    /**
     * Newer analyses (since release X in February 2020) can have multiple cutoffs.
     * If this is non-null it completely supersedes cutoffMinutes, which should be ignored.
     */
    public int[] cutoffsMinutes;

    /**
     * A geometry defining the bounds of this regional analysis.
     * For now, we will use the bounding box of this geometry, but eventually we should figure out which
     * points should be included and only include those points. When you have an irregular analysis region,
     * See also: https://commons.wikimedia.org/wiki/File:The_Gerry-Mander_Edit.png
     */
    public Geometry bounds;

    /** Is this Analysis complete? */
    public boolean complete;

    /** Has this analysis been (soft) deleted? */
    public boolean deleted;

    public RegionalAnalysis clone () {
        try {
            return (RegionalAnalysis) super.clone();
        } catch (CloneNotSupportedException e) {
            throw AnalysisServerException.unknown(e);
        }
    }
}
