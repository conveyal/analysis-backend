package com.conveyal.r5.analyst.cluster;

import com.conveyal.r5.analyst.FreeFormPointSet;
import com.conveyal.r5.analyst.PointSet;
import com.conveyal.r5.analyst.PointSetCache;
import com.conveyal.r5.analyst.WebMercatorExtents;
import com.conveyal.r5.analyst.WebMercatorGridPointSetCache;
import com.conveyal.r5.analyst.WorkerCategory;
import com.conveyal.r5.analyst.decay.DecayFunction;
import com.conveyal.r5.profile.ProfileRequest;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Describes an analysis task to be performed for a single origin, on its own or as part of a multi-origin analysis.
 * Instances are serialized and sent from the backend to workers.
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    // these match the enum values in AnalysisWorkerTask.Type
    @JsonSubTypes.Type(name = "TRAVEL_TIME_SURFACE", value = TravelTimeSurfaceTask.class),
    @JsonSubTypes.Type(name = "REGIONAL_ANALYSIS", value = RegionalTask.class)
})
public abstract class AnalysisWorkerTask extends ProfileRequest {

    /**
     * The largest number of cutoffs we'll accept in a task. Too many cutoffs can create very large output files.
     * Note that when we begin calculating single-point accessibility on the worker instead of in the UI, there will
     * frequently be 120 cutoffs. Then this limit will only apply to regional analyses, not single-point.
     */
    public static final int MAX_CUTOFFS = 120; // WORK IN PROGRESS: worker side accessibility (should be lower)

    /** The largest number of percentiles we'll accept in a task. */
    public static final int MAX_PERCENTILES = 5;

    /**
     * A loading cache of gridded pointSets (not opportunity data grids), shared statically among all single point and
     * multi-point (regional) requests. TODO make this non-static yet shared.
     */
    public static final WebMercatorGridPointSetCache gridPointSetCache = new WebMercatorGridPointSetCache();

    // Extents of a web Mercator grid. Unfortunately this grid serves different purposes in different requests.
    // In the single-origin TravelTimeSurfaceTasks, the grid points are the destinations.
    // In regional multi-origin tasks, the grid points are the origins, with destinations determined by the selected
    // opportunity dataset.
    // In regional Taui (static site) tasks the grid points serve as both the origins and the destinations.
    public int zoom;
    public int west;
    public int north;
    public int width;
    public int height;

    /** The ID of the graph against which to calculate this task. */
    public String graphId;

    /** The commit of r5 the worker should be running when it processes this task. */
    public String workerVersion;

    /**
     * The job ID this is associated with.
     * TODO it seems like this should be equal to RegionalAnalysis._id, and be called regionalAnalysisId.
     *      This seems to be only relevant for regional tasks but is on the shared superclass (push it down?)
     */
    public String jobId;

    /** The id of this particular origin. */
    public String originId;

    /**
     * A unique identifier for this task assigned by the queue/broker system.
     * TODO This seems to be only relevant for regional tasks but is on the shared superclass (push it down?)
     */
    public int taskId;

    /**
     * Whether to save results on S3.
     * If false, the results will only be sent back to the broker or UI.
     * If true, travel time surfaces and paths will be saved to S3
     * Currently this only works on regional requests, and causes them to produce travel time surfaces instead of
     * accessibility indicator values.
     * FIXME in practice this currently implies computeTravelTimeBreakdown and computePaths, so we've got redundant and
     * potentially incoherent information in the request. The intent is in the future to make all these options
     * separate - we can make either travel time surfaces or accessibility indicators or both, and they may or may
     * not be saved to S3.
     */
    public boolean makeTauiSite = false;

    /** Whether to break travel time down into in-vehicle, wait, and access/egress time. */
    public boolean computeTravelTimeBreakdown = false;

    /** Whether to include paths in results. This allows rendering transitive-style schematic maps. */
    public boolean computePaths = false;

    /**
     * Which percentiles of travel time to calculate.
     * These should probably just be integers, but there are already a lot of them in Mongo as floats.
     */
    public int[] percentiles;

    /**
     * The travel time cutoffs in minutes for regional accessibility analysis.
     * A single cutoff was previously determined by superclass field ProfileRequest.maxTripDurationMinutes.
     * That field still cuts off the travel search at a certain number of minutes, so it is set to the highest cutoff.
     * Note this will only be set for accessibility calculation tasks, not for travel time surfaces.
     * TODO move it to the regional task subclass?
     */
    public int[] cutoffsMinutes;

    /**
     * When recording paths as in a static site, how many distinct paths should be saved to each destination?
     * Currently this only makes sense in regional tasks, but it could also become relevant for travel time surfaces
     * so it's in this superclass.
     */
    public int nPathsPerTarget = 3;

    /**
     * Whether the R5 worker should log an analysis request it receives from the broker. analysis-backend translates
     * front-end requests to the format expected by R5. To debug this translation process, set logRequest = true in
     * the front-end profile request, then look for the full request received by the worker in its Cloudwatch log.
     */
    public boolean logRequest = false;

    /**
     * The distance decay function applied to make more distant opportunities
     * Deserialized into various subtypes from JSON.
     */
    public DecayFunction decayFunction;

    /**
     * The storage keys for the pointsets we will compute access to. The format is regionId/datasetId.fileFormat.
     * Ideally we'd just provide the IDs of the grids, but creating the key requires us to know the region
     * ID and file format, which are not otherwise easily available.
     */
    public String[] destinationPointSetKeys;

    /**
     * The pointsets we are calculating accessibility to, including opportunity density data (not bare sets of points).
     * Note that these are transient - they are not serialized and sent over the wire, they are loaded by the worker
     * from the storage location specified by the destinationPointSetKeys.
     *
     * For now, if the destinations are grids, there may be more than one but they must have the same WebMercatorExtents;
     * if the destinations are freeform points, only a single pointset is allowed.
     */
    public transient PointSet[] destinationPointSets;

    /**
     * Is this a single point or regional request? Needed to encode types in JSON serialization. Can that type field be
     * added automatically with a serializer annotation instead of by defining a getter method and two dummy methods?
     */
    public abstract Type getType();

    /** Ensure that type is perceived as a field by serialization libs, no-op */
    public void setType (Type type) {};
    public void setTypes (String type) {};

    /**
     * @return extents for the appropriate destination grid, derived from task's bounds and zoom (for Taui site tasks
     * and single-point travel time surface tasks) or destination pointset (for standard regional accessibility
     * analysis tasks)
     */
    @JsonIgnore
    public abstract WebMercatorExtents getWebMercatorExtents();

    @JsonIgnore
    public WorkerCategory getWorkerCategory () {
        return new WorkerCategory(graphId, workerVersion);
    }

    public enum Type {
        /*
           TODO we should not need this enum - this should be handled automatically by JSON serializer annotations.
           These could also be changed to SINGLE_POINT and MULTI_POINT. The type of results requested (i.e. a grid of
           travel times per origin vs. an accessibility value per origin) can be inferred based on whether grids are
           specified in the profile request.  If travel time results are requested, flags can specify whether components
           of travel time (e.g. waiting) and paths should also be returned.
         */
        /** Binary grid of travel times from a single origin, for multiple percentiles, returned via broker by default */
        TRAVEL_TIME_SURFACE,
        /** Cumulative opportunity accessibility values for all cells in a grid, returned to broker for assembly*/
        REGIONAL_ANALYSIS
    }

    public AnalysisWorkerTask clone () {
        // no need to catch CloneNotSupportedException, it's caught in ProfileRequest::clone
        return (AnalysisWorkerTask) super.clone();
    }

    /**
     * @return the expected number of destination points for this particular kind of task.
     */
    public abstract int nTargetsPerOrigin();

    /**
     * Populate this task's transient array of destination PointSets, loading each destination PointSet key from the
     * supplied cache. The PointSets themselves are not serialized and sent over to the worker in the task, so this
     * method is called by the worker to materialize them.
     *
     * For now, this requires grids with the same extents. Future enhancements could first create a stack of aligned
     * targets, with appropriate indexing.
     *
     * TODO merge multiple destination pointsets from a regional request into a single supergrid?
     */
    public void loadAndValidateDestinationPointSets (PointSetCache pointSetCache) {
        checkNotNull(destinationPointSetKeys);
        int nPointSets = destinationPointSetKeys.length;
        checkState(nPointSets > 0 && nPointSets <= 10,
                "You must specify at least 1 destination PointSet, but no more than 10.");
        destinationPointSets = new PointSet[nPointSets];
        for (int i = 0; i < nPointSets; i++) {
            PointSet pointSet = pointSetCache.get(destinationPointSetKeys[i]);
            checkNotNull(pointSet, "Could not load PointSet specified in regional task.");
            destinationPointSets[i] = pointSet;
            if (pointSet instanceof FreeFormPointSet) {
                checkArgument(nPointSets == 1,
                        "If a freeform destination PointSet is specified, it must be the only one.");
            } else {
                checkArgument(pointSet.getWebMercatorExtents().equals(destinationPointSets[0].getWebMercatorExtents()),
                        "If multiple grids are specified as destinations, they must have identical extents.");
            }
        }
    }

    public void validatePercentiles () {
        checkNotNull(percentiles);
        int nPercentiles = percentiles.length;
        checkArgument(nPercentiles <= MAX_PERCENTILES, "Maximum number of percentiles allowed is " + MAX_PERCENTILES);
        for (int p = 0; p < nPercentiles; p++) {
            checkArgument(percentiles[p] > 0 && percentiles[p] < 100, "Percentiles must be in range [1, 99].");
            if (p > 0) {
                checkState(percentiles[p] >= percentiles[p - 1], "Percentiles must be in ascending order.");
            }
        }
    }

    public void validateCutoffsMinutes () {
        checkNotNull(cutoffsMinutes);
        int nCutoffs = cutoffsMinutes.length;
        checkArgument(nCutoffs >= 1, "At least one cutoff must be supplied.");
        checkArgument(nCutoffs <= MAX_CUTOFFS, "Maximum number of cutoffs allowed is " + MAX_CUTOFFS);
        for (int c = 0; c < nCutoffs; c++) {
            checkArgument(cutoffsMinutes[c] >= 0, "Cutoffs must be non-negative integers.");
            checkArgument(cutoffsMinutes[c] <= 120, "Cutoffs must be at most 120 minutes.");
            if (c > 0) {
                checkArgument(cutoffsMinutes[c] >= cutoffsMinutes[c - 1], "Cutoffs must be in ascending order.");
            }
        }
    }

}
