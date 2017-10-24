package com.conveyal.taui.grids;

import com.conveyal.data.census.S3SeamlessSource;
import com.conveyal.data.geobuf.GeobufFeature;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.models.Project;
import gnu.trove.map.TObjectDoubleMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Fetch data from the seamless-census s3 buckets and convert it from block-level vector data (polygons)
 * to raster opportunity density data (grids).
 */
public class SeamlessCensusGridExtractor extends GridExtractor {

    public static final Logger LOG = LoggerFactory.getLogger(SeamlessCensusGridExtractor.class);

    // The Web Mercator zoom level of the census data grids that will be created.
    public static final int ZOOM = 9;

    /** Generally set from JSON deserialization but can be set directly in caller code. */
    public String sourceBucket;

    /**
     * The main entry point for this GridCreator. Pulls vector data from seamless-census and turns it into
     * rasters. Returns a list of all the different attributes of the census data, each of which will yield
     * a separate grid.
     *
     * The status callback will be called periodically to return the status of the fetch to the API.
     */
    public List<Project.OpportunityDataset> extractData (String targetBucket, String s3prefix,
                                                         double north, double east, double south, double west,
                                                         Consumer<Project.LoadStatus> statusCallback) {
        long startTime = System.currentTimeMillis();

        statusCallback.accept(Project.LoadStatus.DOWNLOADING_CENSUS);

        S3SeamlessSource source = new S3SeamlessSource(sourceBucket);
        Map<Long, GeobufFeature> features;
        try {
            // All the features are buffered in a Map in memory. This could be problematic on large areas.
            features = source.extract(north, east, south, west, false);
        } catch (IOException e) {
            // This will happen if you don't have access to this bucket on S3.
            throw new RuntimeException(e);
        }

        if (features.isEmpty()) {
            LOG.info("No seamless census data found here, not prepopulating grids");
            return Collections.emptyList();
        }

        // One string naming each attribute (column) in the incoming census data.
        Set<String> attributes = new HashSet<>();

        for (GeobufFeature feature : features.values()) {
            feature.properties.entrySet()
                    .stream()
                    .filter(e -> e.getValue() instanceof Number)
                    .forEach(e -> attributes.add(e.getKey()));
        }

        // Make an empty grid for each attribute of the source census data.
        statusCallback.accept(Project.LoadStatus.EXTRACTING_CENSUS_COLUMNS);
        Map<String, Grid> gridForAttribute = attributes.stream()
                .collect(Collectors.toMap(k -> k, k -> new Grid(ZOOM, north, east, south, west)));

        // Features are unique because they've been put in a map keyed on feature ID.
        // Loop over the features only once here, burning each attribute of the feature into the corresponding grid.
        // We previously looped over features multiple times, once for each attribute, but that was very slow.
        statusCallback.accept(Project.LoadStatus.PROJECTING_CENSUS);
        int featIdx = 0;
        for (GeobufFeature feature : features.values()) {
            if (++featIdx % 1000 == 0) LOG.info("{} / {} features read", featIdx, features.size());

            TObjectDoubleMap<int[]> weights = null;

            for (String attribute : attributes) {

                Number value = (Number) feature.properties.get(attribute);
                if (value == null) continue;

                Grid grid = gridForAttribute.get(attribute);

                // grids for each attribute are identical in size, do geographic math once and cache for whole feature
                if (weights == null) {
                    weights = grid.getPixelWeights(feature.geometry);
                }

                grid.incrementFromPixelWeights(weights, value.doubleValue());
            }
        }

        statusCallback.accept(Project.LoadStatus.STORING_CENSUS);

        // Write all the resulting grids out to gzipped objects on S3, and make a list of model objects for them.
        List<Project.OpportunityDataset> opportunityDatasets = new ArrayList<>();
        gridForAttribute.forEach((attribute, grid) -> {
            String cleanedAttributeName = attribute
                    .replaceAll(" ", "_")
                    .replaceAll("[^a-zA-Z0-9_]", "");

            try {
                // First write out the grid itself to an object on S3.
                String outKey = String.format("%s/%s.grid", s3prefix, cleanedAttributeName);
                OutputStream os = getOutputStream(targetBucket, outKey);
                grid.write(os);
                os.close();
                // Also write out a PNG as a preview of the grid's contents.
                // TODO remove this once debugging not necessary.
                String outPng = String.format("%s/%s.png", s3prefix, cleanedAttributeName);
                os = getOutputStream(targetBucket, outPng);
                grid.writePng(os);
                os.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Create an object representing this new destination density grid in the Analysis backend internal model.
            Project.OpportunityDataset opportunityDataset = new Project.OpportunityDataset();
            opportunityDataset.dataSource = this.name;
            opportunityDataset.name = attribute;
            opportunityDataset.key = cleanedAttributeName;
            opportunityDatasets.add(opportunityDataset);
        });

        long endTime = System.currentTimeMillis();
        LOG.info("Extracting Census data took {} seconds", (endTime - startTime) / 1000);

        // Return an internal model object for each census grid that was created.
        return opportunityDatasets;
    }
}
