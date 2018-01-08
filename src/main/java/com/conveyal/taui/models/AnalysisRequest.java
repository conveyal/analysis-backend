package com.conveyal.taui.models;

import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
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

public class AnalysisRequest {
    private static int ZOOM = 9;

    public String projectId;
    public int variantIndex;
    public String workerVersion;

    // All analyses parameters
    public String accessModes;
    public float bikeSpeed;
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

    // Parameters that aren't currently configurable in the UI
    public int bikeTrafficStress = 4;
    public float carSpeed = 20;
    public int maxWalkTime = 20;
    public int maxBikeTime = 20;
    public int maxCarTime = 45;
    public int minBikeTime = 10;
    public int minCarTime = 10;
    public int streetTime = 90;
    public int suboptimalMinutes = 5;

    // Regional only
    public Bounds bounds;
    public Integer maxTripDurationMinutes;
    public String name;
    public String opportunityDatasetKey;
    public Integer travelTimePercentile;

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
     * TODO do we really need to pass in a base AnalysisTask? can't we construct a fresh AnalysisTask in this method?
     */
    public AnalysisTask populateTask (AnalysisTask task, Project project) {
        List<Modification> modifications = new ArrayList<>();

        // No modifications in the baseline comparison
        if (variantIndex > -1) {
            modifications = modificationsForProject(project.accessGroup, projectId, variantIndex);
        }

        // No idea how long this operation takes or if it is actually necessary
        CRC32 crc = new CRC32();
        crc.update(modifications.stream().map(Modification::toString).collect(Collectors.joining("-")).getBytes());
        crc.update(JsonUtilities.objectToJsonBytes(this));

        task.scenario = new Scenario();
        // TODO figure out why we use both
        task.jobId = String.format("%s-%s-%s", projectId, variantIndex, crc.getValue());
        task.scenario.id = task.scenarioId = task.jobId;
        task.scenario.modifications = modifications;

        task.graphId = project.bundleId;
        task.workerVersion = workerVersion;

        Bounds b = bounds;
        if (b == null) {
            Region region = Persistence.regions.findByIdIfPermitted(project.regionId, project.accessGroup);
            b = region.bounds;
        }

        int east = Grid.lonToPixel(b.east, ZOOM);
        int north = Grid.latToPixel(b.north, ZOOM);
        int south = Grid.latToPixel(b.south, ZOOM);
        int west = Grid.lonToPixel(b.west, ZOOM);

        task.height = south - north;
        task.north = north;
        task.west = west;
        task.width = east - west;
        task.zoom = ZOOM;

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

        if (travelTimePercentile == null) {
            task.percentiles = new double[]{5, 25, 50, 75, 95};
        } else {
            task.percentiles = new double[]{travelTimePercentile};
        }

        if (maxTripDurationMinutes != null) {
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
