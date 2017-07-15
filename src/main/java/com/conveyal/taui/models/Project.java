package com.conveyal.taui.models;

import com.conveyal.taui.util.HttpUtil;
import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.grids.SeamlessCensusGridExtractor;
import com.conveyal.taui.persistence.OSMPersistence;
import com.conveyal.taui.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.io.ByteStreams;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Represents a project, which is a set of GTFS, OSM, and land use data for a particular location.
 */
public class Project extends Model implements Cloneable {
    private static final Logger LOG = LoggerFactory.getLogger(Project.class);

    // Keep track of project load status in memory, don't go back and forth to the DB
    // not a memory leak, since when projects are done they are removed from the map.
    public static Map<String, LoadStatus> loadStatusForProject = new ConcurrentHashMap<>();

    /** Project name */
    public String name;

    /** Project description */
    public String description;

    /** R5 version used for analysis */
    public String r5Version;

    /** Bounds of this project */
    public Bounds bounds;

    /** Group this project is associated with */
    public String group;

    /** Does this project use custom OSM */
    public boolean customOsm;

    public Boolean showGeocoder;

    private static SeamlessCensusGridExtractor gridFetcher = new SeamlessCensusGridExtractor();
    static {
        gridFetcher.sourceBucket = AnalystConfig.seamlessCensusBucket;
        gridFetcher.name = AnalystConfig.seamlessCensusBucket;
    }

    public LoadStatus getLoadStatus () {
        return loadStatusForProject.getOrDefault(id, LoadStatus.DONE);
    }

    public void setLoadStatus (LoadStatus status) {
        // do nothing, because we can't mark getLoadStatus as jsonignore because it has to go to the API, and
        // and JsonViews don't work in MongoJack . . . *sigh*
    }

    // don't persist to DB but do expose to API
    @JsonView(JsonViews.Api.class)
    public List<Bundle> getBundles () {
        return Persistence.bundles.values()
                .stream()
                .filter(b -> id.equals(b.projectId))
                .collect(Collectors.toList());
    }

    @JsonView(JsonViews.Api.class)
    public List<Scenario> getScenarios () {
        return Persistence.scenarios.values()
                .stream()
                .filter(s -> id.equals(s.projectId))
                .collect(Collectors.toList());
    }

    @JsonView(JsonViews.Api.class)
    public Collection<Bookmark> getBookmarks () {
        return Persistence.bookmarks.getByProperty("projectId", id);
    }

    @JsonView(JsonViews.Api.class)
    public Collection<Mask> getMasks () {
        return Persistence.masks.getByProperty("projectId", id);
    }

    public List<Indicator> indicators = new ArrayList<>();

    public synchronized void fetchOsm () throws IOException {
        loadStatusForProject.put(id, LoadStatus.DOWNLOADING_OSM);
        File temporaryFile = File.createTempFile("osm", ".pbf");
        String url = String.format(Locale.US, "%s/%f,%f,%f,%f.pbf", AnalystConfig.vexUrl,
                bounds.south,
                bounds.west,
                bounds.north,
                bounds.east);

        HttpGet get = new HttpGet(url);
        CloseableHttpResponse res = null;
        try {
            res = HttpUtil.httpClient.execute(get);

            if (res.getStatusLine().getStatusCode() != 200) {
                LOG.info("Could not retrieve OSM for project {}", this.name);
            }

            InputStream is = res.getEntity().getContent();
            OutputStream fos = new BufferedOutputStream(new FileOutputStream(temporaryFile));
            ByteStreams.copy(is, fos);
            fos.close();
            is.close();
            EntityUtils.consume(res.getEntity());

            OSMPersistence.cache.put(this.id, temporaryFile);
            temporaryFile.delete();
        } finally {
            if (res != null) res.close();
        }

        // TODO remove all cached transport networks for this project
    }

    public synchronized void fetchCensus () {
        this.indicators.addAll(gridFetcher.extractData(AnalystConfig.gridBucket, this.id,
                bounds.north,
                bounds.east,
                bounds.south,
                bounds.west,
                status -> loadStatusForProject.put(id, status)
                ));
    }

    public Project clone () {
        try {
            return (Project) super.clone();
        } catch (CloneNotSupportedException e) {
            // can't happen.
            throw new RuntimeException(e);
        }
    }

    public static class Bounds {
        public double north, east, south, west;

        @Override
        public boolean equals (Object other) {
            return equals(other, 0D);
        }

        public boolean equals (Object other, double tolerance) {
            if (!Bounds.class.isInstance(other)) return false;
            Bounds o = (Bounds) other;
            return Math.abs(north - o.north) <= tolerance && Math.abs(east - o.east) <= tolerance &&
                    Math.abs(south - o.south) <= tolerance && Math.abs(west - o.west) <= tolerance;
        }
    }

    /** Represents an indicator */
    public static class Indicator {
        /** The human-readable name of the data source from which this came */
        public String dataSource;

        /** The human readable name of the indicator */
        public String name;

        /** The key on S3 */
        public String key;
    }

    /** Represents the status of OSM and Census loading */
    public enum LoadStatus {
        /** Downloading OpenStreetMap data */
        DOWNLOADING_OSM,

        /** Downloading Census data using seamless-census */
        DOWNLOADING_CENSUS,

        /** Figuring out which census columns are numeric */
        EXTRACTING_CENSUS_COLUMNS,

        /** Projecting census features into grids */
        PROJECTING_CENSUS,

        /** Storing census data on S3 */
        STORING_CENSUS,
        DONE
    }
}
