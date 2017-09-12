package com.conveyal.taui;

import com.auth0.jwt.JWTVerifier;
import com.conveyal.gtfs.GTFSCache;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.taui.analysis.LocalCluster;
import com.conveyal.taui.controllers.*;
import com.conveyal.taui.persistence.OSMPersistence;
import com.conveyal.taui.persistence.Persistence;
import com.google.common.io.CharStreams;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.Map;

import static com.conveyal.taui.util.SparkUtil.haltWithJson;
import static spark.Spark.*;

/**
 * Main entry point
 */
public class TransportAnalyst {
    private static final Logger LOG = LoggerFactory.getLogger(TransportAnalyst.class);

    public static void main (String... args) throws Exception {
        LOG.info("Starting TAUI server at {}", LocalDateTime.now());

        byte[] auth0Secret = new Base64(true).decode(AnalystConfig.auth0Secret);
        String auth0ClientId = AnalystConfig.auth0ClientId;

        LOG.info("Connecting to database");
        Persistence.initialize();

        LOG.info("Initializing GTFS cache");
        File cacheDir = new File(AnalystConfig.localCache);
        cacheDir.mkdirs();
        GTFSCache gtfsCache = new GTFSCache(AnalystConfig.offline ? null : AnalystConfig.bundleBucket, new File(AnalystConfig.localCache));
        ApiMain.initialize(gtfsCache);

        LOG.info("Starting server");
        port(AnalystConfig.port);

        // initialize ImageIO
        // http://stackoverflow.com/questions/20789546
        ImageIO.scanForPlugins();

        // serve up index.html which pulls client code from S3


        // check if a user is authenticated
        before((req, res) -> {
            if (!req.pathInfo().startsWith("/api")) return; // don't need to be authenticated to view main page

            if (!AnalystConfig.offline) {
                String auth = req.headers("Authorization");

                // authorization required
                if (auth == null || auth.isEmpty()) {
                    haltWithJson(401, "You must be logged in.");
                }

                // make sure it's properly formed
                String[] authComponents = auth.split(" ");

                if (authComponents.length != 2 || !"bearer".equals(authComponents[0].toLowerCase())) {
                    haltWithJson(400, "Authorization header is malformed: " + auth);
                }

                // validate the JWT
                JWTVerifier verifier = new JWTVerifier(auth0Secret, auth0ClientId);

                Map<String, Object> jwt = null;
                try {
                    jwt = verifier.verify(authComponents[1]);
                } catch (Exception e) {
                    LOG.info("Login failed", e);
                    haltWithJson(403, "Login failed to verify with our authorization provider.");
                }

                if (!jwt.containsKey("analyst")) {
                    haltWithJson(403, "Access denied. User does not have access to Analysis.");
                }

                String group = null;
                try {
                    group = (String) ((Map<String, Object>) jwt.get("analyst")).get("group");
                } catch (Exception e) {
                    haltWithJson(403, "Access denied. User is not associated with any group.");
                }

                if (group == null) haltWithJson(403, "Access denied. User is not associated with any group.");

                req.attribute("group", group);
            } else {
                // hardwire group name if we're working offline
                req.attribute("group", "OFFLINE");
            }

            // Default is JSON, will be overridden by the few controllers that do not return JSON
            res.type("application/json");
        });

        ProjectController.register();
        ModificationController.register();
        ScenarioController.register();
        GraphQLController.register();
        BundleController.register();
        SinglePointAnalysisController.register();
        GridController.register();
        RegionalAnalysisController.register();
        AggregationAreaController.register();

        // load and serve index.html

        InputStream indexStream = TransportAnalyst.class.getClassLoader().getResourceAsStream("public/index.html");
        String index = CharStreams.toString(new InputStreamReader(indexStream)).replace("${ASSET_LOCATION}", AnalystConfig.assetLocation);
        indexStream.close();

        get("/*", (req, res) -> { res.type("text/html"); return index; });

        if (AnalystConfig.offline) {
            LOG.info("Starting local cluster");
            // TODO port is hardwired here and also in SinglePointAnalysisController
            // You have to make the worker machineId non-static if you want to launch more than one worker.
            new LocalCluster(6001, gtfsCache, OSMPersistence.cache, 1);
        }

        LOG.info("Transport Analyst is ready");
    }
}
