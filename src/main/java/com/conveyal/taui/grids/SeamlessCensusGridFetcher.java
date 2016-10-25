package com.conveyal.taui.grids;

import com.conveyal.data.census.S3SeamlessSource;
import com.conveyal.data.geobuf.GeobufFeature;
import com.conveyal.taui.models.Project;
import com.google.common.collect.Lists;
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

/**
 * Fetch data from seamless-census s3 buckets
 */
public class SeamlessCensusGridFetcher extends GridFetcher {
    public static final Logger LOG = LoggerFactory.getLogger(SeamlessCensusGridFetcher.class);

    public static final int ZOOM = 9;

    /** generally set from JSON deserialization but can be set imperatively */
    public String sourceBucket;

    public final String type = "seamless";

    public List<Project.Indicator> extractData (String targetBucket, String prefix, double north, double east, double south, double west) {
        S3SeamlessSource source = new S3SeamlessSource(sourceBucket);
        Map<Long, GeobufFeature> features;
        try {
            features = source.extract(north, east, south, west, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (features.isEmpty()) {
            LOG.info("No seamless census data found here, not prepopulating grids");
            return Collections.emptyList();
        }

        Set<String> attributes = new HashSet<>();

        for (GeobufFeature feature : features.values()) {
            feature.properties.entrySet()
                    .stream()
                    .filter(e -> e.getValue() instanceof Number)
                    .forEach(e -> attributes.add(e.getKey()));
        }

        List<Project.Indicator> indicators = new ArrayList<>();

        for (String attribute : attributes) {
            Grid grid = new Grid(ZOOM, north, east, south, west);

            for (GeobufFeature feature : features.values()) {
                Number value = (Number) feature.properties.get(attribute);
                if (value == null) continue;

                // features are unique because they've been put in a map by ID
                grid.rasterize(feature.geometry, value.doubleValue());
            }

            String attributeClean = attribute
                    .replaceAll(" ", "_")
                    .replaceAll("[^a-zA-Z0-9_]", "");

            String outKey = String.format("%s/%s.grid", prefix, attributeClean);
            String outPng = String.format("%s/%s.png", prefix, attributeClean);

            try {
                OutputStream os = getOutputStream(targetBucket, outKey);
                grid.write(os);
                os.close();

                // TODO do we need this long term?
                /*os = getOutputStream(targetBucket, outPng);
                grid.writePng(os);
                os.close();*/
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Project.Indicator indicator = new Project.Indicator();
            indicator.dataSource = this.name;
            indicator.name = attribute;
            indicator.key = attributeClean;
            indicators.add(indicator);
        }

        return indicators;
    }
}
