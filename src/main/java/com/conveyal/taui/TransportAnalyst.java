package com.conveyal.taui;

import com.conveyal.taui.controllers.ModificationController;
import com.conveyal.taui.controllers.ScenarioController;
import com.conveyal.taui.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Date;

import static spark.Spark.after;

/**
 * Main entry point
 */
public class TransportAnalyst {
    private static final Logger LOG = LoggerFactory.getLogger(TransportAnalyst.class);

    public static void main (String... args) {
        LOG.info("Starting TAUI server at {}", LocalDateTime.now());

        LOG.info("Connecting to database");
        Persistence.initialize();

        LOG.info("Starting server");
        ModificationController.register();
        ScenarioController.register();

        // TODO FIXME very bad once we have any sort of authentication
        after((req, res) -> {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Methods", "PUT,POST");
        });
    }
}
