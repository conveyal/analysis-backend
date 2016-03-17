package com.conveyal.taui;

import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.taui.controllers.BundleController;
import com.conveyal.taui.controllers.GraphQLController;
import com.conveyal.taui.controllers.ModificationController;
import com.conveyal.taui.controllers.ScenarioController;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import static spark.Spark.after;
import static spark.SparkBase.port;

/**
 * Main entry point
 */
public class TransportAnalyst {
    private static final Logger LOG = LoggerFactory.getLogger(TransportAnalyst.class);

    public static Properties config;

    public static void main (String... args) throws Exception {
        LOG.info("Starting TAUI server at {}", LocalDateTime.now());

        LOG.info("Reading configuration");
        config = new Properties();
        // TODO don't hardwire
        FileInputStream in = new FileInputStream(new File("application.conf"));
        config.load(in);
        in.close();

        LOG.info("Connecting to database");
        Persistence.initialize();

        // TODO hack
        ApiMain.feedSources = new HashMap<>();

        LOG.info("Starting server");
        port(7070);
        ModificationController.register();
        ScenarioController.register();
        GraphQLController.register();
        BundleController.register();

        Bundle.load();

        // TODO FIXME very bad once we have any sort of authentication
        after((req, res) -> {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Methods", "PUT,POST");
        });
    }
}
