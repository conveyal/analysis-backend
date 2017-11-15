package com.conveyal.taui.grids;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.data.census.S3SeamlessSource;
import com.conveyal.data.geobuf.GeobufFeature;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.models.Bounds;
import com.conveyal.taui.models.Project;
import gnu.trove.map.TObjectDoubleMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * Fetch data from the seamless-census s3 buckets and convert it from block-level vector data (polygons)
 * to raster opportunity density data (grids).
 */
public class SeamlessCensusGridExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(SeamlessCensusGridExtractor.class);

    private static final String gridBucket = AnalysisServerConfig.gridBucket;
    private static final String seamlessCensusBucket = AnalysisServerConfig.seamlessCensusBucket;

    // The Web Mercator zoom level of the census data grids that will be created.
    public static final int ZOOM = 9;

    // A pool of threads that upload newly created grids to S3. The pool is shared between all GridFetchers.
    // The default policy when the pool's work queue is full is to abort with an exception.
    // We shouldn't use the caller-runs policy because that will cause deadlocks.
    private static final ThreadPoolExecutor s3Upload = new ThreadPoolExecutor(4, 8, 90, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024));
    private static final AmazonS3 s3 = new AmazonS3Client();

    /** Prepare an outputstream on S3, set up to gzip and upload whatever is uploaded in a thread. */
    private static OutputStream getOutputStream (String s3Key) throws IOException {
        PipedOutputStream outputStream = new PipedOutputStream();
        PipedInputStream inputStream = new PipedInputStream(outputStream);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/octet-stream");
        metadata.setContentEncoding("gzip");
        PutObjectRequest request = new PutObjectRequest(gridBucket, s3Key, inputStream, metadata);

        // upload to s3 in a separate thread so that we don't deadlock
        s3Upload.execute(() -> s3.putObject(request));

        return new GZIPOutputStream(outputStream);
    }

    /**
     * Retrieve data for bounds and save to a bucket under a given key
     */
    public static List<Project.OpportunityDataset> retrieveAndExtractCensusDataForBounds (Bounds bounds, String s3Key) throws IOException {
        long startTime = System.currentTimeMillis();

        S3SeamlessSource source = new S3SeamlessSource(seamlessCensusBucket);
        Map<Long, GeobufFeature> features;

        // All the features are buffered in a Map in memory. This could be problematic on large areas.
        features = source.extract(bounds.north, bounds.east, bounds.south, bounds.west, false);

        if (features.isEmpty()) {
            LOG.info("No seamless census data found here, not pre-populating grids");
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
        Map<String, Grid> gridForAttribute = attributes
                .stream()
                .collect(Collectors.toMap(
                        k -> k,
                        k -> new Grid(ZOOM, bounds.north, bounds.east, bounds.south, bounds.west)));

        // Features are unique because they've been put in a map keyed on feature ID.
        // Loop over the features only once here, burning each attribute of the feature into the corresponding grid.
        // We previously looped over features multiple times, once for each attribute, but that was very slow.
        int featIdx = 0;
        for (GeobufFeature feature : features.values()) {
            if (++featIdx % 1000 == 0) LOG.info("{} / {} features read", featIdx, features.size());

            List weights = null;

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

        // Write all the resulting grids out to gzipped objects on S3, and make a list of model objects for them.
        List<Project.OpportunityDataset> opportunityDatasets = new ArrayList<>();
        for (Map.Entry<String, Grid> entry : gridForAttribute.entrySet()) {
            String attribute = entry.getKey();
            Grid grid = entry.getValue();

            String cleanedAttributeName = attribute
                    .replaceAll(" ", "_")
                    .replaceAll("[^a-zA-Z0-9_]", "");

            // First write out the grid itself to an object on S3.
            String outKey = String.format("%s/%s.grid", s3Key, cleanedAttributeName);
            OutputStream os = getOutputStream(outKey);
            grid.write(os);
            os.close();
            // Also write out a PNG as a preview of the grid's contents.
            // TODO remove this once debugging not necessary.
            String outPng = String.format("%s/%s.png", s3Key, cleanedAttributeName);
            os = getOutputStream(outPng);
            grid.writePng(os);
            os.close();

            // Create an object representing this new destination density grid in the Analysis backend internal model.
            Project.OpportunityDataset opportunityDataset = new Project.OpportunityDataset();
            opportunityDataset.dataSource = seamlessCensusBucket;
            opportunityDataset.name = attribute;
            opportunityDataset.key = cleanedAttributeName;
            opportunityDatasets.add(opportunityDataset);
        }

        long endTime = System.currentTimeMillis();
        LOG.info("Extracting Census data took {} seconds", (endTime - startTime) / 1000);

        // Return an internal model object for each census grid that was created.
        return opportunityDatasets;
    }
}
