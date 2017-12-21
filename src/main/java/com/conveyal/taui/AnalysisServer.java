package com.conveyal.taui;

import com.auth0.jwt.JWTVerifier;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.util.FeedSourceCache;
import com.conveyal.r5.analyst.broker.Broker;
import com.conveyal.taui.analysis.LocalCluster;
import com.conveyal.taui.controllers.AggregationAreaController;
import com.conveyal.taui.controllers.BundleController;
import com.conveyal.taui.controllers.GraphQLController;
import com.conveyal.taui.controllers.ModificationController;
import com.conveyal.taui.controllers.OpportunityDatasetsController;
import com.conveyal.taui.controllers.RegionController;
import com.conveyal.taui.controllers.RegionalAnalysisController;
import com.conveyal.taui.controllers.ProjectController;
import com.conveyal.taui.controllers.SinglePointAnalysisController;
import com.conveyal.taui.controllers.WorkerController;
import com.conveyal.taui.persistence.OSMPersistence;
import com.conveyal.taui.persistence.Persistence;
import com.google.common.io.CharStreams;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

import static spark.Spark.before;
import static spark.Spark.exception;
import static spark.Spark.get;
import static spark.Spark.port;

/**
 * This is the main entry point for starting a Conveyal Analysis server.
 */
public class AnalysisServer {

    private static final Logger LOG = LoggerFactory.getLogger(AnalysisServer.class);

    public static void main (String... args) {
        LOG.info("Starting Conveyal Analysis server, the time is now {}", DateTime.now());

        LOG.info("Connecting to database...");
        Persistence.initialize();

        LOG.info("Initializing local GTFS cache...");
        File cacheDir = new File(AnalysisServerConfig.localCache);
        cacheDir.mkdirs();

        // Set up Spark

        // Set the port on which the HTTP server will listen for connections.
        LOG.info("Analysis server will listen for HTTP connections on port {}.", AnalysisServerConfig.port);
        port(AnalysisServerConfig.port);

        // initialize ImageIO
        // http://stackoverflow.com/questions/20789546
        ImageIO.scanForPlugins();

        // Before handling each request, check if the user is authenticated.
        before((req, res) -> {
            if (!req.pathInfo().startsWith("/api")) return; // don't need to be authenticated to view main page

            // Default is JSON, will be overridden by the few controllers that do not return JSON
            res.type("application/json");

            if (AnalysisServerConfig.auth0ClientId != null && AnalysisServerConfig.auth0Secret != null) {
                handleAuthentication(req, res);
            } else {
                // LOG.warn("No Auth0 credentials were supplied, setting accessGroup and email to placeholder defaults");
                // hardwire group name if we're working offline
                req.attribute("accessGroup", "OFFLINE");
                req.attribute("email", "analysis@conveyal.com");
            }

            // Log each API request
            LOG.info("{} {} by {} of {}", req.requestMethod(), req.pathInfo(), req.attribute("email"), req.attribute("accessGroup"));
        });

        // Register all our HTTP request handlers with the Spark HTTP framework.
        RegionController.register();
        ModificationController.register();
        ProjectController.register();
        GraphQLController.register();
        BundleController.register();
//        SinglePointAnalysisController.register();
        OpportunityDatasetsController.register();
        RegionalAnalysisController.register();
        AggregationAreaController.register();


        // TODO wire up Spark without using static methods:
//        spark.Service httpService = spark.Service.ignite()
//                .port(1234)
//                .staticFileLocation("/public")
//                .threadPool(40);
//
//        httpService.get("/hello", (q, a) -> "Hello World!");
//        httpService.get("/goodbye", (q, a) -> "Goodbye!");
//        httpService.redirect.any("/hi", "/hello");

        // Add a controller to handle connections from workers, using the local broker
        Properties brokerConfig = new Properties();
        // I believe work-offline tells the broker not to spin up AWS instances.
        brokerConfig.setProperty("work-offline", "true");
        brokerConfig.setProperty("bind-address", "localhost");
        brokerConfig.setProperty("port", "" + AnalysisServerConfig.port);
        Broker broker = new Broker(brokerConfig, "localhost", AnalysisServerConfig.port);
        new WorkerController(broker).register();


        // Load index.html and register a handler with Spark to serve it up.
        InputStream indexStream = AnalysisServer.class.getClassLoader().getResourceAsStream("public/index.html");

        try {
            String index = CharStreams.toString(
                    new InputStreamReader(indexStream)).replace("${ASSET_LOCATION}", AnalysisServerConfig.assetLocation);
            indexStream.close();

            get("/*", (req, res) -> {
                res.type("text/html");
                return index;
            });
        } catch (IOException e) {
            LOG.error("Unable to load index.html");
            System.exit(1);
        }

        exception(AnalysisServerException.class, (e, request, response) -> {
            AnalysisServerException ase = ((AnalysisServerException) e);
            AnalysisServer.respondToException(ase, request, response, ase.type.name(), ase.message, ase.httpCode);
        });

        exception(IOException.class, (e, request, response) -> {
            AnalysisServer.respondToException(e, request, response, "BAD_REQUEST", e.toString(), 400);
        });

        exception(FileUploadException.class, (e, request, response) -> {
            AnalysisServer.respondToException(e, request, response, "BAD_REQUEST", e.toString(), 400);
        });

        exception(NullPointerException.class, (e, request, response) -> {
            AnalysisServer.respondToException(e, request, response, "UNKNOWN", e.toString(), 400);
        });

        exception(RuntimeException.class, (e, request, response) -> {
            AnalysisServer.respondToException(e, request, response, "RUNTIME", e.toString(), 400);
        });

        if (AnalysisServerConfig.offline) {
            LOG.info("Running in OFFLINE mode...");
            FeedSourceCache feedSourceCache = ApiMain.initialize(null, AnalysisServerConfig.localCache);

            LOG.info("Starting local cluster of Analysis workers...");
            // TODO port is hardwired here and also in SinglePointAnalysisController
            // You have to make the worker machineId non-static if you want to launch more than one worker.
            LocalCluster localCluster = new LocalCluster(7070, feedSourceCache, OSMPersistence.cache, 1);
        } else {
            ApiMain.initialize(AnalysisServerConfig.bundleBucket, AnalysisServerConfig.localCache);
        }


        LOG.info("Conveyal Analysis server is ready.");
    }

    public static void handleAuthentication (Request req, Response res) {
        String auth = req.headers("Authorization");

        // authorization required
        if (auth == null || auth.isEmpty()) {
            throw AnalysisServerException.Unauthorized("You must be logged in.");
        }

        // make sure it's properly formed
        String[] authComponents = auth.split(" ");

        if (authComponents.length != 2 || !"bearer".equals(authComponents[0].toLowerCase())) {
            throw AnalysisServerException.Unknown("Authorization header is malformed: " + auth);
        }

        // validate the JWT
        JWTVerifier verifier = new JWTVerifier(AnalysisServerConfig.auth0Secret, AnalysisServerConfig.auth0ClientId);

        Map<String, Object> jwt = null;
        try {
            jwt = verifier.verify(authComponents[1]);
        } catch (Exception e) {
            throw AnalysisServerException.Forbidden("Login failed to verify with our authorization provider. " + e.getMessage());
        }

        if (!jwt.containsKey("analyst")) {
            throw AnalysisServerException.Forbidden("Access denied. User does not have access to Analysis.");
        }

        String group = null;
        try {
            group = (String) ((Map<String, Object>) jwt.get("analyst")).get("group");
        } catch (Exception e) {
            throw AnalysisServerException.Forbidden("Access denied. User is not associated with any group. " + e.getMessage());
        }

        if (group == null) {
            throw AnalysisServerException.Forbidden("Access denied. User is not associated with any group.");
        }

        // attributes to be used on models
        req.attribute("accessGroup", group);
        req.attribute("email", jwt.get("email"));
    }

    public static void respondToException(Exception e, Request request, Response response, String type, String message, int code) {
        String stack = ExceptionUtils.getStackTrace(e);

        LOG.error("{} {} -> {} {} by {} of {}", type, message, request.requestMethod(), request.pathInfo(), request.attribute("email"), request.attribute("accessGroup"));
        LOG.error(stack);

        JSONObject body = new JSONObject();
        body.put("type", type);
        body.put("message", message);
        body.put("stackTrace", stack);

        response.status(code);
        response.type("application/json");
        response.body(body.toJSONString());
    }
}
