package com.conveyal.taui.controllers;

import com.conveyal.r5.analyst.cluster.TravelTimeSurfaceTask;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.models.AnalysisRequest;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.HttpUtil;
import com.google.common.io.ByteStreams;
import org.apache.http.client.methods.CloseableHttpResponse;
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

import static spark.Spark.post;

/**
 * Handles talking to the broker.
 */
public class SinglePointAnalysisController {
    private static final Logger LOG = LoggerFactory.getLogger(SinglePointAnalysisController.class);
    private static final String BROKER_ENQUEUE_SINGLE_URL = AnalysisServerConfig.brokerUrl + "/enqueue/single";

    public static byte[] analysis (Request req, Response res) throws IOException {
        // we already know the user is authenticated, and we need not check if they have access to the graphs etc,
        // as they're all coded with UUIDs which contain significantly more entropy than any human's account password.
        final String accessGroup = req.attribute("accessGroup");
        final String email = req.attribute("email");

        AnalysisRequest analysisRequest = JsonUtilities.objectMapper.readValue(req.body(), AnalysisRequest.class);
        Project project = Persistence.projects.findByIdIfPermitted(analysisRequest.projectId, accessGroup);
        TravelTimeSurfaceTask task = (TravelTimeSurfaceTask) analysisRequest.populateTask(new TravelTimeSurfaceTask(), project);


        LOG.info("Single point request by {} made {}", email, BROKER_ENQUEUE_SINGLE_URL);

        CloseableHttpResponse brokerRes = null;
        HttpPost post = new HttpPost(BROKER_ENQUEUE_SINGLE_URL);
        // We're ignoring the content type of the incoming request and forcing it to JSON
        // which should be fine since the broker's single point endpoint always expects POST bodies to be JSON.
        // We do need to force the encoding to utf-8 here otherwise multi-byte characters get corrupted.
        post.setEntity(new StringEntity(JsonUtilities.objectMapper.writeValueAsString(task), ContentType.create("application/json", "utf-8")));
        try {
            brokerRes = HttpUtil.httpClient.execute(post);
            res.status(brokerRes.getStatusLine().getStatusCode());
            res.type(brokerRes.getFirstHeader("Content-Type").getValue());
            // TODO set encoding?
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = new BufferedInputStream(brokerRes.getEntity().getContent());
            long l = ByteStreams.copy(is, baos);
            is.close();
            EntityUtils.consume(brokerRes.getEntity());

            LOG.info("Returning {} bytes to the frontend", l);
            return baos.toByteArray();
        } finally {
            if (brokerRes != null) brokerRes.close();
        }
    }

    public static void register () {
        post("/api/analysis/", SinglePointAnalysisController::analysis);
    }
}
