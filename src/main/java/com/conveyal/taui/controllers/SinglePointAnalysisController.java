package com.conveyal.taui.controllers;

import com.conveyal.taui.AnalystConfig;
import com.google.common.io.ByteStreams;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.http.conn.HttpHostConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;

import static spark.Spark.*;

/**
 * Handles talking to the broker.
 */
public class SinglePointAnalysisController {
    public static final Logger LOG = LoggerFactory.getLogger(SinglePointAnalysisController.class);

    public static byte[] analysis (Request req, Response res) throws UnirestException {
        // we already know the user is authenticated, and we need not check if they have access to the graphs etc,
        // as they're all coded with UUIDs which contain significantly more entropy than any human's account password.
        String path = req.pathInfo().replaceAll("^/api/analysis/", "");
        String method = req.requestMethod();

        String brokerUrl = AnalystConfig.offline ? "http://localhost:6001" : AnalystConfig.brokerUrl;

        HttpResponse<InputStream> brokerRes;

        try {
            if ("GET".equals(method)) {
                brokerRes = Unirest.get(brokerUrl + "/" + path)
                        .asBinary();
            } else if ("POST".equals(method)) {
                brokerRes = Unirest.post(brokerUrl + "/" + path)
                        .body(req.body())
                        .asBinary();
            } else if ("DELETE".equals(method)) {
                brokerRes = Unirest.delete(brokerUrl + "/" + path)
                        .asBinary();
            } else {
                halt(400, "Unsupported method for broker request");
                return null;
            }

            res.status(brokerRes.getStatus());
            res.type(brokerRes.getHeaders().getFirst("Content-Type"));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = brokerRes.getBody();

            try {
                long l = ByteStreams.copy(is, baos);
                LOG.info("Returning {} bytes to scenario editor frontend", l);
                is.close();
            } catch (IOException e) {
                LOG.error("Error proxying broker", e);
            }

            return baos.toByteArray();
        } catch (UnirestException e) {
            LOG.error("analysis error", e.getCause());
            if (e.getCause() instanceof SocketTimeoutException) {
                halt(504, "Timeout contacting broker");
            } else if (e.getCause() instanceof HttpHostConnectException) {
                halt(503, "Broker not available");
            } else {
                halt(500, "Could not contact broker");
            }
        }

        return null;
    }

    public static void register () {
        // TODO is there a way to do a wildcard that includes slashes?
        // Also, are there any broker endpoints that don't have two path components.
        post("/api/analysis/*/*", SinglePointAnalysisController::analysis);
        get("/api/analysis/*/*", SinglePointAnalysisController::analysis);
        delete("/api/analysis/*/*", SinglePointAnalysisController::analysis);
    }
}
