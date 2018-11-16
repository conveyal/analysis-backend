package com.conveyal.taui.grids;

import com.conveyal.data.census.S3SeamlessSource;
import com.conveyal.data.geobuf.GeobufFeature;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.models.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fetch data from the seamless-census s3 buckets and convert it from block-level vector data (polygons)
 * to raster opportunity density data (grids).
 */
public class SeamlessCensusGridExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(SeamlessCensusGridExtractor.class);

    // The Web Mercator zoom level of the census data grids that will be created.
    public static final int ZOOM = 9;

    private static S3SeamlessSource source = new S3SeamlessSource(AnalysisServerConfig.seamlessCensusRegion,
            AnalysisServerConfig.seamlessCensusBucket);

    /**
     * Retrieve data for bounds and save to a bucket under a given key
     */
    public static Map<String, Grid> retrieveAndExtractCensusDataForBounds (Bounds bounds) throws IOException {
        long startTime = System.currentTimeMillis();

        // All the features are buffered in a Map in memory. This could be problematic on large areas.
        Map<Long, GeobufFeature> features = source.extract(bounds.north, bounds.east, bounds.south, bounds.west, false);

        if (features.isEmpty()) {
            LOG.info("No seamless census data found here, not pre-populating grids");
            return new HashMap<>();
        }

        // One string naming each attribute (column) in the incoming census data.
        Map<String, Grid> grids = new HashMap<>();
        features.values().stream().forEach(feature -> {
            List weights = null;
            // Iterate over all of the features
            for (Map.Entry<String, Object> e : feature.properties.entrySet()) {
                if (!(e.getValue() instanceof Number)) continue;
                Number value = (Number) e.getValue();
                String key = e.getKey();

                Grid grid = grids.get(key);
                if (grid == null) {
                    grid = new Grid(ZOOM, bounds.north, bounds.east, bounds.south, bounds.west);
                    grids.put(key, grid);
                }

                if (weights == null) {
                    weights = grid.getPixelWeights(feature.geometry);
                }

                grid.incrementFromPixelWeights(weights, value.doubleValue());
            }
        });

        long endTime = System.currentTimeMillis();
        LOG.info("Extracting Census data took {} seconds", (endTime - startTime) / 1000);

        return grids;
    }
}
