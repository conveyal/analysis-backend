package com.conveyal.taui.models;

import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.persistence.OSMPersistence;
import com.google.common.io.ByteStreams;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;

/**
 * Represents a project, which is a set of GTFS, OSM, and land use data for a particular location.
 */
public class Project extends Model {
    private static final Logger LOG = LoggerFactory.getLogger(Project.class);

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

    public void fetchOsm () throws IOException, UnirestException {
        File temporaryFile = File.createTempFile("osm", ".pbf");
        String url = String.format(Locale.US, "%s/%f,%f,%f,%f.pbf", AnalystConfig.vexUrl,
                bounds.south,
                bounds.west,
                bounds.north,
                bounds.east);

        HttpResponse<InputStream> res = Unirest.get(url)
                .asBinary();

        if (res.getStatus() != 200) {
            LOG.info("Could not retrieve OSM for project {}", this.name);
        }

        InputStream is = res.getBody();
        FileOutputStream fos = new FileOutputStream(temporaryFile);
        ByteStreams.copy(is, fos);
        fos.close();
        is.close();

        OSMPersistence.cache.put(this.id, temporaryFile);
        temporaryFile.delete();
    }

    public static class Bounds {
        public double north, east, south, west;
    }
}
