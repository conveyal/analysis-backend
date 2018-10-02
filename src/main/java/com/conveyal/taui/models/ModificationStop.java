package com.conveyal.taui.models;

import com.conveyal.r5.analyst.scenario.StopSpec;
import com.conveyal.r5.util.ExceptionUtils;
import com.conveyal.taui.AnalysisServerException;
import com.vividsolutions.jts.geom.Coordinate;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

class ModificationStop {

    // Workaround for the fact that id = null was used for both auto-generated stops and new stops not referencing
    // GTFS stop IDs. We give the auto-generated stops this instance as their ID, and check it with identity equality.
    // As long as we don't let these escape from this class, there's no risk of accidentally semantically equaling a
    // real stop ID from a feed.
    public static final String AUTO_GENERATED_STOP_ID = "AUTO_GENERATED_STOP";

    private static double MIN_SPACING_PERCENTAGE = 0.25;
    private static int DEFAULT_SEGMENT_SPEED = 15;
    public static int SECONDS_PER_HOUR = 60 * 60;
    public static int METERS_PER_KM = 1000;

    private Coordinate coordinate;
    private String id; // null means a newly created stop that does not reference a GTFS stop
    private double distanceFromStart;

    private ModificationStop(Coordinate c, String id, double distanceFromStart) {
        this.coordinate = c;
        this.id = id;
        this.distanceFromStart = distanceFromStart;
    }

    /**
     * Convert ModificationStops (internal to the backend conversion process) to the StopSpec type required by r5.
     */
    static List<StopSpec> toStopSpecs (List<ModificationStop> stops) {
        return stops
                .stream()
                .map(s -> {
                    if (s.id == null || s.id == AUTO_GENERATED_STOP_ID){
                        // Stop is newly created or auto-generated so does not reference a GTFS stop ID.
                        return new StopSpec(s.coordinate.x, s.coordinate.y);
                    } else {
                        // Stop references an existing GTFS stop ID.
                        return new StopSpec(s.id);
                    }
                })
                .collect(Collectors.toList());
    }

    /**
     * Ideally we'd just convert directly to R5 StopSpec instances, but we need to track distanceFromStart for
     * generating hop times from speeds. See conveyal/analysis-backend#175 for a cleaner solution.
     */
    static List<ModificationStop> getStopsFromSegments (List<Segment> segments) {

        // Keep a stack of Stops because as part of auto-generating stops we sometimes need to back one out.
        Stack<ModificationStop> stops = new Stack<>();
        CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84;

        if (segments == null || segments.size() == 0) {
            return new ArrayList<>();
        }

        Segment firstSegment = segments.get(0);

        if (firstSegment.stopAtStart) {
            stops.add(new
                    ModificationStop(firstSegment.geometry.getCoordinates()[0], firstSegment.fromStopId, 0));
        }

        double distanceToLastStop = 0; // distance to previously created stop, from start of pattern
        double distanceToLineSegmentStart = 0; // from start of pattern
        for (Segment segment : segments) {
            Coordinate[] coords = segment.geometry.getCoordinates();
            int spacing = segment.spacing;
            boolean autoCreateStops = spacing > 0;
            for (int i = 1; i < coords.length; i++) {
                Coordinate c0 = coords[i - 1];
                Coordinate c1 = coords[i];
                double distanceThisLineSegment;
                try {
                    // JTS orthodromic distance returns meters, considering the input coordinate system.
                    distanceThisLineSegment = JTS.orthodromicDistance(c0, c1, crs);
                } catch (TransformException e) {
                    throw AnalysisServerException.unknown(ExceptionUtils.asString(e));
                }
                if (autoCreateStops) {
                    // Auto-create stops while this segment includes at least one point that is more than 'spacing'
                    // meters farther along the pattern than the previously created stop
                    while (distanceToLastStop + spacing < distanceToLineSegmentStart + distanceThisLineSegment) {
                        double frac = (distanceToLastStop + spacing - distanceToLineSegmentStart) / distanceThisLineSegment;
                        if (frac < 0) frac = 0;
                        Coordinate c = new Coordinate(c0.x + (c1.x - c0.x) * frac, c0.y + (c1.y - c0.y) * frac);

                        // We can't just add segment.spacing because of converting negative fractions to zero above.
                        // This can happen when the last segment did not have automatic stop creation, or had a larger
                        // spacing. TODO in the latter case, we probably want to continue to apply the spacing from the
                        // last line segment until we create a new stop?
                        distanceToLastStop = distanceToLineSegmentStart + frac * distanceThisLineSegment;

                        // Add the auto-created stop with a special ID that can be checked with identity equality
                        stops.add(new ModificationStop(c, AUTO_GENERATED_STOP_ID, distanceToLastStop));
                    }
                }

                distanceToLineSegmentStart += distanceThisLineSegment;
            }

            if (segment.stopAtEnd) {
                // If the last auto-generated stop was too close to a manually created stop (other than the first stop),
                // remove it.
                if (autoCreateStops && stops.size() > 1) {
                    ModificationStop lastStop = stops.peek();
                    if (lastStop.id == AUTO_GENERATED_STOP_ID
                            && (distanceToLineSegmentStart - distanceToLastStop) / spacing < MIN_SPACING_PERCENTAGE) {
                        stops.pop();
                    }
                }

                Coordinate endCoord = coords[coords.length - 1];
                ModificationStop toStop = new ModificationStop(endCoord, segment.toStopId, distanceToLineSegmentStart);
                stops.add(toStop);
                // restart the spacing
                distanceToLastStop = distanceToLineSegmentStart; // distanceToLineSegmentStart was already set to the next line segment
            }
        }

        return new ArrayList<>(stops);
    }

    static int[] getDwellTimes (List<ModificationStop> stops, Integer[] dwellTimes, int defaultDwellTime) {
        if (stops == null || stops.size() == 0) {
            return new int[0];
        }

        int[] stopDwellTimes = new int[stops.size()];

        // This "real" stop index is the index into the originally supplied stops, ignoring the auto-generated ones.
        int realStopIndex = 0;
        for (int i = 0; i < stops.size(); i++) {
            String id = stops.get(i).id;
            if (id == AUTO_GENERATED_STOP_ID || dwellTimes == null || dwellTimes.length <= realStopIndex) {
                stopDwellTimes[i] = defaultDwellTime;
            } else {
                Integer specificDwellTime = dwellTimes[realStopIndex];
                stopDwellTimes[i] = specificDwellTime != null ? specificDwellTime : defaultDwellTime;
                realStopIndex++;
            }
        }

        return stopDwellTimes;
    }

    static int[] getHopTimes (List<ModificationStop> stops, int[] segmentSpeedsKph) {
        if (stops == null || stops.size() < 2) {
            return new int[0];
        }

        int[] hopTimesSeconds = new int[stops.size() - 1];
        ModificationStop lastStop = stops.get(0);

        // This "real" stop index is the index into the originally supplied stops, ignoring the auto-generated ones.
        int realStopIndex = 0;
        for (int i = 0; i < hopTimesSeconds.length; i++) {
            ModificationStop stop = stops.get(i + 1);
            double hopDistance = stop.distanceFromStart - lastStop.distanceFromStart;

            int segmentSpeedKph = segmentSpeedsKph.length > realStopIndex ? segmentSpeedsKph[realStopIndex] :
                    DEFAULT_SEGMENT_SPEED;
            hopTimesSeconds[i] = (int) (hopDistance / (segmentSpeedKph * METERS_PER_KM) * SECONDS_PER_HOUR);

            if (stop.id != AUTO_GENERATED_STOP_ID) {
                realStopIndex++;
            }

            lastStop = stop;
        }

        return hopTimesSeconds;
    }
}
