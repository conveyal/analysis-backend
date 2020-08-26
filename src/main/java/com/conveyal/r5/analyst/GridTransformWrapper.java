package com.conveyal.r5.analyst;

import org.locationtech.jts.geom.Envelope;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This wraps a gridded destination pointset, remapping its point indexes to match those of another pointset.
 * This can be used to stack pointsets of different dimensions, or to calculate accessibility to pointsets of
 * different dimensions than a travel time surface grid.
 *
 * TODO this is also more or less what we need to support multiple grid dimensions in
 *  loadAndValidateDestinationPointSets(), which would then be able to work for both single point and regional requests.
 *  In fact single point and regional tasks are slowly converging and will ideally eventually be very similar.
 */
public class GridTransformWrapper extends PointSet {

    private WebMercatorGridPointSet targetPointSet;
    private Grid sourceGrid;

    /**
     * Wraps the sourceGrid such that the location of a given point index will match that in the targetPointSet, but the
     * opportunity count is read from the cell at the same geographic location in the sourceGrid.
     * For the time being, both pointsets must be at the same zoom level. Any opportunities outside the targetPointSet
     * cannot be indexed so are effectively zero for the purpose of accessibility calculations.
     */
    public GridTransformWrapper (WebMercatorGridPointSet targetPointSet, Grid sourceGrid) {
        checkArgument(targetPointSet.zoom == sourceGrid.zoom, "Zoom levels must be identical.");
        this.targetPointSet = targetPointSet;
        this.sourceGrid = sourceGrid;
    }

    // Given an index in the targetPointSet, return the corresponding 1D index into the sourceGrid or -1 if the target
    // index is for a point outside the source grid.
    // This could certainly be made more efficient (but complex) by forcing sequential iteration over opportunity counts
    // and disallowing random access, using a new PointSetIterator class that allows reading lat, lon, and counts.
    private int transformIndex (int i) {
        final int x = (i % targetPointSet.width) + targetPointSet.west - sourceGrid.west;
        final int y = (i / targetPointSet.width) + targetPointSet.north - sourceGrid.north;
        if (x < 0 || x >= sourceGrid.width || y < 0 || y >= sourceGrid.height) {
            // Point in target grid lies outside source grid, there is no valid index. Return special value.
            return -1;
        }
        return y * sourceGrid.width + x;
    }

    @Override
    public double getLat (int i) {
        return targetPointSet.getLat(i);
    }

    @Override
    public double getLon (int i) {
        return targetPointSet.getLon(i);
    }

    @Override
    public int featureCount () {
        return targetPointSet.featureCount();
    }

    @Override
    public double sumTotalOpportunities() {
        // Very inefficient compared to the other implementations as it does a lot of index math, but it should work.
        double totalOpportunities = 0;
        for (int i = 0; i < featureCount(); i++) {
            totalOpportunities += getOpportunityCount(i);
        }
        return totalOpportunities;
    }

    @Override
    public double getOpportunityCount (int i) {
        int targetindex = transformIndex(i);
        if (targetindex < 0) {
            return 0;
        } else {
            return sourceGrid.getOpportunityCount(targetindex);
        }
    }

    @Override
    public Envelope getWgsEnvelope () {
        return targetPointSet.getWgsEnvelope();
    }

    @Override
    public WebMercatorExtents getWebMercatorExtents () {
        return targetPointSet.getWebMercatorExtents();
    }

}
