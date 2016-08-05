package com.conveyal.taui.controllers;

import com.conveyal.taui.AnalystConfig;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

/**
 * Handles talking to the broker.
 */
public class AnalysisController {
    public static String analysis (Request req, Response res) throws UnirestException {
        // we already know the user is authenticated, and we need not check if they have access to the graphs etc,
        // as they're all coded with UUIDs which contain significantly more entropy than any human's account password.
        String path = req.pathInfo().replaceAll("^/api/analysis/", "");
        String method = req.requestMethod();

        res.type("application/json");

        String brokerUrl = AnalystConfig.offline ? "http://localhost:6001" : AnalystConfig.brokerUrl;

        if ("GET".equals(method)) {
            HttpResponse<String> brokerRes = Unirest.get(brokerUrl + "/" + path)
                    .asString();

            res.status(brokerRes.getStatus());
            return brokerRes.getBody();
        } else if ("POST".equals(method)) {
            HttpResponse<String> brokerRes = Unirest.post(brokerUrl + "/" + path)
                    .body(req.body())
                    .asString();

            res.status(brokerRes.getStatus());
            return brokerRes.getBody();
        } else if ("DELETE".equals(method)) {
            HttpResponse<String> brokerRes = Unirest.delete(brokerUrl + "/" + path)
                    .asString();

            res.status(brokerRes.getStatus());
            return brokerRes.getBody();
        } else {
            halt(400, "Unsupported method for broker request");
            return null;
        }
    }

    public static void register () {
        // TODO is there a way to do a wildcard that includes slashes?
        // Also, are there any broker endpoints that don't have two path components.
        post("/api/analysis/*/*", AnalysisController::analysis);
        get("/api/analysis/*/*", AnalysisController::analysis);
        delete("/api/analysis/*/*", AnalysisController::analysis);
    }
}
