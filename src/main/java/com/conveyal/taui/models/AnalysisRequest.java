package com.conveyal.taui.models;

import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
import com.conveyal.r5.analyst.fare.InRoutingFareCalculator;
import com.conveyal.r5.analyst.scenario.Modification;
import com.conveyal.r5.analyst.scenario.Scenario;
import com.conveyal.r5.api.util.LegMode;
import com.conveyal.r5.api.util.TransitModes;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.persistence.Persistence;
import com.mongodb.QueryBuilder;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * This is the request sent from the UI. It is actually distinct from the requests sent to the R5 workers, though it
 * has many of the same fields.
 */
public class AnalysisRequest {
    private static int ZOOM = 9;

    public String projectId;
    public int variantIndex;
    public String workerVersion;

    // All analyses parameters
    public String accessModes;
    public float bikeSpeed;
    public Bounds bounds;
    public LocalDate date;
    public String directModes;
    public String egressModes;
    public float fromLat;
    public float fromLon;
    public int fromTime;
    public int monteCarloDraws = 200;
    public int toTime;
    public String transitModes;
    public float walkSpeed;
    public int maxRides = 4;
    public double[] percentiles;
    public int maxWalkTime = 20;
    public int maxBikeTime = 20;

    // Parameters that aren't currently configurable in the UI
    public int bikeTrafficStress = 4;
    public float carSpeed = 20;
    public int maxCarTime = 45;
    public int minBikeTime = 10;
    public int minCarTime = 10;
    public int streetTime = 90;
    public int suboptimalMinutes = 5;

    // Regional only
    public Integer maxTripDurationMinutes;
    public String name;
    public String opportunityDatasetId;
    public Integer travelTimePercentile;
    // Save all results in a regional analysis to S3 for display in a "static site".
    public boolean makeStaticSite = false;
    public int maxFare;
    public InRoutingFareCalculator inRoutingFareCalculator;

    /**
     * Get all of the modifications for a project id that are in the Variant and map them to their corresponding r5 mod
     */
    private static List<Modification> modificationsForProject (String accessGroup, String projectId, int variantIndex) {
        return Persistence.modifications
                .findPermitted(QueryBuilder.start("projectId").is(projectId).get(), accessGroup)
                .stream()
                .filter(m -> variantIndex < m.variants.length && m.variants[variantIndex])
                .map(com.conveyal.taui.models.Modification::toR5)
                .collect(Collectors.toList());
    }

    /**
     * Finds the modifications for the specified project and variant, maps them to their corresponding R5 modification
     * types, creates a checksum from those modifications, and adds them to the AnalysisTask along with the rest of the
     * request.
     *
     * This method takes a task as a parameter, modifies that task, and also returns that same task.
     * This is because we have two subtypes of AnalysisTask and need to be able to create both.
     */
    public AnalysisTask populateTask (AnalysisTask task, Project project) {
        List<Modification> modifications = new ArrayList<>();

        // No modifications in the baseline comparison
        if (variantIndex > -1) {
            modifications = modificationsForProject(project.accessGroup, projectId, variantIndex);
        }

        // The CRC is appended to the scenario ID to identify a unique revision of the scenario (still denoted here
        // as variant) allowing the worker to cache and reuse networks built by applying that exact revision of the
        // scenario to a base network.
        CRC32 crc = new CRC32();
        crc.update(JsonUtilities.objectToJsonBytes(modifications));
        long crcValue = crc.getValue();

        task.scenario = new Scenario();
        // TODO figure out why we use both
        // (AB: what does the above comment mean? both what?)
        // FIXME Job IDs need to be unique. Why are we setting this to the project and variant? This only works because the job ID is overwritten when the job is enqueued.
        task.jobId = String.format("%s-%s-%s", projectId, variantIndex, crcValue);
        task.scenario.id = task.scenarioId = task.jobId;
        task.scenario.modifications = modifications;
        task.graphId = project.bundleId;
        task.workerVersion = workerVersion;
        task.maxFare = this.maxFare;
        task.inRoutingFareCalculator = this.inRoutingFareCalculator;

        Bounds bounds = this.bounds;
        if (bounds == null) {
            // If no bounds were speicified, fall back on the bounds of the entire region.
            Region region = Persistence.regions.findByIdIfPermitted(project.regionId, project.accessGroup);
            bounds = region.bounds;
        }

        // TODO define class with static factory function WebMercatorGridBounds.fromLatLonBounds(). Also include getIndex(x, y), getX(index), getY(index), totalTasks()
        Grid grid = new Grid(ZOOM, bounds.north, bounds.east, bounds.south, bounds.west);
        task.height = grid.height;
        task.north = grid.north;
        task.west = grid.west;
        task.width = grid.width;
        task.zoom = grid.zoom;

        task.date = date;
        task.fromLat = fromLat;
        task.fromLon = fromLon;
        task.fromTime = fromTime;
        task.toTime = toTime;

        task.bikeSpeed = bikeSpeed;
        task.carSpeed = carSpeed;
        task.walkSpeed = walkSpeed;

        task.bikeTrafficStress = bikeTrafficStress;
        task.maxWalkTime = maxWalkTime;
        task.maxBikeTime = maxBikeTime;
        task.maxCarTime = maxCarTime;
        task.maxRides = maxRides;
        task.minBikeTime = minBikeTime;
        task.minCarTime = minCarTime;
        task.streetTime = streetTime;
        task.suboptimalMinutes = suboptimalMinutes;

        task.monteCarloDraws = monteCarloDraws;
        task.percentiles = percentiles;

        // TODO FIXME HUGE HACK
        // The max time cutoff really speeds along fare-based requests, so we inject max trip duration minutes into the
        // request if it has a fare calculator assigned. This does mean that isochrone results will be invalid if the user
        // moves the slider up after making a fare-based request.
        // It is also done for regional requests (as it always was)
        if (task.getType() == AnalysisTask.Type.REGIONAL_ANALYSIS || task.inRoutingFareCalculator != null) {
            task.maxTripDurationMinutes = maxTripDurationMinutes;
        }

        task.accessModes = getEnumSetFromString(accessModes);
        task.directModes = getEnumSetFromString(directModes);
        task.egressModes = getEnumSetFromString(egressModes);
        task.transitModes = transitModes != null && !"".equals(transitModes)
                ? EnumSet.copyOf(Arrays.stream(transitModes.split(",")).map(TransitModes::valueOf).collect(Collectors.toList()))
                : EnumSet.noneOf(TransitModes.class);

        return task;
    }

    private EnumSet<LegMode> getEnumSetFromString (String s) {
        if (s != null && !"".equals(s)) {
            return EnumSet.copyOf(Arrays.stream(s.split(",")).map(LegMode::valueOf).collect(Collectors.toList()));
        } else {
            return EnumSet.noneOf(LegMode.class);
        }
    }
}
