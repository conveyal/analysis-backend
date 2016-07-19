package com.conveyal.taui.migrations;

import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.Scenario;
import com.conveyal.taui.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Migrate the flat structure to categorizing scenarios into projects.
 */
public class MigrateToProjects {
    private static final Logger LOG = LoggerFactory.getLogger(MigrateToProjects.class);

    public static void main (String... args) {
        File cacheDir = new File(AnalystConfig.localCache);
        cacheDir.mkdirs();
        ApiMain.initialize(AnalystConfig.bundleBucket, AnalystConfig.localCache);
        Persistence.initialize();

        // we are going to create a project for each bundle
        // we want to make sure the projects and bundles don't have the same ID because project data and bundle data are
        // in the same S3 bucket. ID collisions won't happen currently as extensions differ (.pbf vs. .json) but no
        // point in boxing us in
        Map<String, String> bundleToProjectIdMap = Persistence.bundles.values().stream()
                .collect(Collectors.toMap(b -> b.id, b -> UUID.randomUUID().toString().replace("-", "")));

        List<Bundle> newBundles = new ArrayList<>();

        for (Bundle b : Persistence.bundles.values()) {
            Project project = new Project();
            project.group = b.group;
            project.id = bundleToProjectIdMap.get(b.id);
            project.name = b.name;
            project.description = "";

            project.bounds = new Project.Bounds();
            project.bounds.north = b.centerLat + 0.5;
            project.bounds.south = b.centerLat - 0.5;
            project.bounds.east = b.centerLon + 0.5;
            project.bounds.west = b.centerLon - 0.5;

            Persistence.projects.put(project.id, project);
            b = b.clone();
            b.projectId = bundleToProjectIdMap.get(b.id);

            newBundles.add(b);
        }

        newBundles.forEach(b -> Persistence.bundles.put(b.id, b));

        List<Scenario> scenarios = new ArrayList<>();
        Persistence.scenarios.values().forEach(s -> {
            s = s.clone();
            s.projectId = bundleToProjectIdMap.get(s.bundleId);
            s.group = null;
            scenarios.add(s);
        });

        scenarios.forEach(s -> Persistence.scenarios.put(s.id, s));
    }
}
