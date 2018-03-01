package com.conveyal.taui.models;

import com.conveyal.r5.analyst.scenario.StopSpec;
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
    private static double MIN_SPACING_PERCENTAGE = 0.25;
    private static int DEFAULT_SEGMENT_SPEED = 15;

    private Coordinate coordinate;
    private String id;
    private double distanceFromStart;

    private ModificationStop(Coordinate c, String id, double distanceFromStart) {
        this.coordinate = c;
        this.id = id;
        this.distanceFromStart = distanceFromStart;
    }

    /**
     * Create the StopSpec types required by r5
     * @param stops
     * @return
     */
    static List<StopSpec> toSpec (List<ModificationStop> stops) {
        return stops
                .stream()
                .map(s -> {
                    if (s.id == null){
                        return new StopSpec(s.coordinate.x, s.coordinate.y);
                    } else {
                        return new StopSpec(s.id);
                    }
                })
                .collect(Collectors.toList());
    }

    /**
     * We don't just use `StopSpec`s here because we need to keep the `distanceFromStart` for generating hop times.
     * @param segments Modification segments
     * @return ModificationStop[]
     */
    static List<ModificationStop> getStopsFromSegments (List<Segment> segments) {
        Stack<ModificationStop> stops = new Stack<>();
        CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84;

        if (segments == null || segments.size() == 0) {
            return new ArrayList<>();
        }

        Segment firstSegment = segments.get(0);

        if (firstSegment.stopAtStart) {
            stops.add(new ModificationStop(firstSegment.geometry.getCoordinates()[0], firstSegment.fromStopId, 0));
        }

        double distanceToLastStop = 0; // distance to previously created stop, from start of pattern
        double distanceToLineSegmentStart = 0; // from start of pattern
        for (Segment segment : segments) {
            Coordinate[] coords = segment.geometry.getCoordinates();
            int spacing = segment.spacing;
            for (int i = 1; i < coords.length; i++) {
                Coordinate c0 = coords[i - 1];
                Coordinate c1 = coords[i];
                double distanceThisLineSegment;
                try {
                    distanceThisLineSegment = JTS.orthodromicDistance(c0, c1, crs);
                } catch (TransformException e) {
                    throw AnalysisServerException.Unknown(e.getMessage());
                }

                if (spacing > 0) {
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

                        // Add the auto-created stop without an id
                        stops.add(new ModificationStop(c, null, distanceToLastStop));
                    }
                }

                distanceToLineSegmentStart += distanceThisLineSegment;
            }

            if (segment.stopAtEnd) {
                // If the last auto-generated stop was too close to a manually created stop (other than the first stop), pop it
                if (stops.size() > 1) {
                    ModificationStop lastStop = stops.peek();
                    if (lastStop.id == null && (distanceToLineSegmentStart - distanceToLastStop) / spacing < MIN_SPACING_PERCENTAGE) {
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

        int realStopIndex = 0;
        for (int i = 0; i < stops.size(); i++) {
            String id = stops.get(i).id;
            if (id == null || dwellTimes == null || dwellTimes.length <= realStopIndex) {
                stopDwellTimes[i] = defaultDwellTime;
            } else {
                Integer specificDwellTime = dwellTimes[realStopIndex];
                stopDwellTimes[i] = specificDwellTime != null ? specificDwellTime : defaultDwellTime;
                realStopIndex++;
            }
        }

        return stopDwellTimes;
    }

    static int[] getHopTimes (List<ModificationStop> stops, int[] segmentSpeeds) {
        if (stops == null || stops.size() < 2) {
            return new int[0];
        }

        int[] hopTimes = new int[stops.size() - 1];

        ModificationStop lastStop = stops.get(0);
        int realStopIndex = 0;
        for (int i = 0; i < hopTimes.length; i++) {
            ModificationStop stop = stops.get(i + 1);
            double hopDistance = stop.distanceFromStart - lastStop.distanceFromStart;

            int segmentSpeed = segmentSpeeds.length > realStopIndex ? segmentSpeeds[realStopIndex] : DEFAULT_SEGMENT_SPEED;
            hopTimes[i] = (int) (hopDistance / (segmentSpeed * 1000) * 3000);

            if (stop.id != null) {
                realStopIndex++;
            }

            lastStop = stop;
        }

        return hopTimes;
    }
}
