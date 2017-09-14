package com.conveyal.taui.controllers;

import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.util.HttpUtil;
import com.google.common.io.ByteStreams;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.conveyal.taui.util.SparkUtil.haltWithJson;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Handles talking to the broker.
 */
public class SinglePointAnalysisController {
    public static final Logger LOG = LoggerFactory.getLogger(SinglePointAnalysisController.class);

    public static byte[] analysis (Request req, Response res) throws IOException {
        // we already know the user is authenticated, and we need not check if they have access to the graphs etc,
        // as they're all coded with UUIDs which contain significantly more entropy than any human's account password.
        String path = req.pathInfo().replaceAll("^/api/analysis/", "");
        String method = req.requestMethod();

        String brokerUrl = AnalystConfig.offline ? "http://localhost:6001" : AnalystConfig.brokerUrl;

        CloseableHttpResponse brokerRes = null;

        try {
            if ("GET".equals(method)) {
                HttpGet get = new HttpGet(brokerUrl + "/" + path);
                brokerRes = HttpUtil.httpClient.execute(get);
            } else if ("POST".equals(method)) {
                HttpPost post = new HttpPost(brokerUrl + "/" + path);
                // We're ignoring the content type of the incoming request and forcing it to JSON
                // which should be fine since the broker's single point endpoint always expects POST bodies to be JSON.
                // We do need to force the encoding to utf-8 here otherwise multi-byte characters get corrupted.
                post.setEntity(new StringEntity(req.body(), ContentType.create("application/json", "utf-8")));
                brokerRes = HttpUtil.httpClient.execute(post);
            } else if ("DELETE".equals(method)) {
                HttpDelete delete = new HttpDelete(brokerUrl + "/" + path);
                brokerRes = HttpUtil.httpClient.execute(delete);
            } else {
                throw new RuntimeException("Unsupported HTTP method on request, not proxying to the broker.");
            }
            res.status(brokerRes.getStatusLine().getStatusCode());
            res.type(brokerRes.getFirstHeader("Content-Type").getValue());
            // TODO set encoding?
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = new BufferedInputStream(brokerRes.getEntity().getContent());
            try {
                long l = ByteStreams.copy(is, baos);
                LOG.info("Returning {} bytes to scenario editor frontend", l);
                is.close();
                EntityUtils.consume(brokerRes.getEntity());
            } catch (Exception e) {
                reportException(e);
            }
            return baos.toByteArray();
        } catch (Exception e) {
            reportException(e);
        } finally {
            if (brokerRes != null) brokerRes.close();
        }
        return null;
    }

    private static void reportException (Exception exception) {
        LOG.error("Uncaught exception: ", exception.toString());
        exception.printStackTrace();
        haltWithJson(500, exception);
    }

    public static void register () {
        // TODO is there a way to do a wildcard that includes slashes?
        // Also, are there any broker endpoints that don't have two path components.
        post("/api/analysis/*/*", SinglePointAnalysisController::analysis);
        get("/api/analysis/*/*", SinglePointAnalysisController::analysis);
        delete("/api/analysis/*/*", SinglePointAnalysisController::analysis);
    }
}
