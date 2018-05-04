package com.conveyal.taui.controllers;

import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.taui.analysis.broker.Broker;
import com.conveyal.r5.analyst.WorkerCategory;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
import com.conveyal.r5.analyst.cluster.RegionalWorkResult;
import com.conveyal.r5.analyst.cluster.TravelTimeSurfaceTask;
import com.conveyal.r5.analyst.cluster.WorkerStatus;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.models.AnalysisRequest;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.HttpStatus;
import com.conveyal.taui.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.List;

import static spark.Spark.get;
import static spark.Spark.head;
import static spark.Spark.post;

/**
 * This is a Spark HTTP controller to handle connections from workers reporting their status and requesting work.
 * This API replaces what used to be a separate broker process.
 *
 * Workers used to long-poll and hold connections open, allowing them to receive tasks instantly, as soon as the tasks
 * are enqueued. However, this adds a lot of complexity since we need to suspend the open connections and clear them
 * out when the connections are closed. I am now having the workers short-poll, which allows a simple, standard
 * HTTP API. We don't care if it takes a few seconds for workers to start consuming regional tasks.
 *
 * The long polling originally only existed for high-priority tasks and has been extended to single point
 * tasks, which we will instead handle with a proxy-like push approach.
 * TODO rename to BrokerController?
 */
public class WorkerController {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerController.class);

    /**
     * The core task queueing and distribution logic is supplied by this broker object.
     * The present class should just wrap that functionality with an HTTP API.
     */
    private Broker broker;

    /** For convenience, a local reference to the shared JSON object codec. */
    private ObjectMapper jsonMapper = JsonUtilities.objectMapper;

    /** This HTTP client contacts workers to send them single-point tasks for immediate processing. */
    private static HttpClient httpClient = AnalystWorker.makeHttpClient();

    public WorkerController (Broker broker) {
        this.broker = broker;
    }

    /**
     * Spark handler functions return Objects.
     * Spark's DefaultSerializer calls toString on those objects and converts the resulting string to UTF-8 bytes.
     * The class spark.webserver.serialization.Serializer only has a few really basic subclasses.
     * In the past we've supplied a ResponseTransformer to these mapping functions to serialize our results.
     * It seems like we should define a Serializer, but Spark provides no setters for MatcherFilter.serializerChain.
     * The other solution (which I've adopted here) is to explicitly serialize our results to JSON and just return
     * a JSON string from our HTTP handler methods.
     */
    public void register () {
        head("", this::headHandler);
        get("/internal/jobs", this::getAllJobs);
        get("/internal/workers", this::getAllWorkers);
        post("/internal/poll", this::workerPoll);
        post("/api/analysis", this::singlePoint); // TODO rename to "single" or something
    }

    /**
     * Handler for single-origin requests. This endpoint is contacted by the frontend rather than the worker, and
     * causes a single-point task to be pushed to an appropriate worker for immediate processing. These requests
     * typically come from an interactive session where the user is moving the origin point around in the web UI.
     * Unlike regional jobs where workers pull tasks from a queue, single point tasks work like a proxy or load balancer.
     * Note that we are using an Apache HTTPComponents client to contact the worker, within a Spark (Jetty) handler.
     * We could use the Jetty HTTP client (which would facilitate exactly replicating request and response headers when
     * we forward the request to a worker), but since Spark wraps the internal Jetty request/response objects, we
     * don't gain much. We should probably switch to the Jetty HTTP client some day when we get rid of Spark.
     * There is also a Jetty proxy module that may be too simple for what we're doing here.
     * @return whatever the worker responds, usually an input stream. Spark serializer chain can properly handle streams.
     */
    private Object singlePoint(Request request, Response response) {
        // Deserialize the task in the request body so we can see what kind of worker it wants.
        // Perhaps we should allow both travel time surface and accessibility calculation tasks to be done as single points.
        // AnalysisRequest (backend) vs. AnalysisTask (R5)
        // The accessgroup stuff is copypasta from the old single point controller.
        // We already know the user is authenticated, and we need not check if they have access to the graphs etc,
        // as they're all coded with UUIDs which contain significantly more entropy than any human's account password.
        final String accessGroup = request.attribute("accessGroup");
        final String userEmail = request.attribute("email");
        AnalysisRequest analysisRequest = objectFromRequestBody(request, AnalysisRequest.class);
        Project project = Persistence.projects.findByIdIfPermitted(analysisRequest.projectId, accessGroup);
        // Transform the analysis UI/backend task format into a slightly different type for R5 workers.
        TravelTimeSurfaceTask task = (TravelTimeSurfaceTask) analysisRequest.populateTask(new TravelTimeSurfaceTask(), project);
        if (request.headers("Accept").equals("image/tiff")) {
            // If the client requested a Geotiff using HTTP headers (for exporting results to GIS),
            // signal this using a field on the request sent to the worker.
            task.setFormat(TravelTimeSurfaceTask.Format.GEOTIFF);
        } else {
            // The default response format is our own compact grid representation.
            task.setFormat(TravelTimeSurfaceTask.Format.GRID);
        }
        WorkerCategory workerCategory = task.getWorkerCategory();
        String address = broker.getWorkerAddress(workerCategory);
        if (address == null) {
            // There are no workers that can handle this request. Request some.
            // FIXME parts of the following method assume that it's synchronized
            broker.createWorkersInCategory(workerCategory, accessGroup, userEmail);
            // No workers exist. Kick one off and return "service unavailable".
            response.header("Retry-After", "30");
            return jsonResponse(response, HttpStatus.ACCEPTED_202, "Starting workers, try again later.");
        } else {
            // Workers exist in this category, clear out any record that we're waiting for one to start up.
            // FIXME the tracking of which workers are starting up should really be encapsulated using a "start up if needed" method.
            broker.recentlyRequestedWorkers.remove(workerCategory);
        }
        String workerUrl = "http://" + address + ":7080/single";
        LOG.info("Re-issuing HTTP request from UI to worker at {}", workerUrl);
        HttpPost httpPost = new HttpPost(workerUrl);
        // httpPost.setHeader("Accept", "application/x-analysis-time-grid");
        httpPost.setHeader("Accept-Encoding", "gzip"); // TODO copy all headers from request? Is this unzipping and re-zipping the result?
        try {
            // Serialize and send the R5-specific task (not the one the UI sends to the broker)
            httpPost.setEntity(new ByteArrayEntity(JsonUtil.objectMapper.writeValueAsBytes(task)));
            HttpResponse workerResponse = httpClient.execute(httpPost);
            response.status(workerResponse.getStatusLine().getStatusCode());
            // TODO Should we exactly mimic these headers coming back from the worker?
            response.header("Content-Type", "application/octet-stream");
            response.header("Content-Encoding", "gzip");
            HttpEntity entity = workerResponse.getEntity();
            LOG.info("Returning worker response to UI.");
            return entity.getContent();
        } catch (Exception e) {
            // TODO we need to detect the case where the worker was not reachable and purge it from the worker catalog.
            return jsonResponse(response, HttpStatus.SERVER_ERROR_500, "Exception while talking to worker: " + e.toString());
        }
    }


    /**
     * TODO respond to HEAD requests. For some reason we needed to implement HEAD, for proxy or cache?
     */
    private Object headHandler(Request request, Response response) {
        return null;
    }

    /**
     * This method factors out a few lines of common code that appear in all handlers that produce JSON responses.
     * Set the response status code and media type, then serialize an object to JSON to serve as a response.
     *
     * @param response the response object on which to set the status code and media type
     * @param statusCode the HTTP status code for this result
     * @param object the object to be serialized
     * @return the serialized object as a String, which can then be returned from the Spark handler function
     */
    private String jsonResponse (Response response, int statusCode, Object object) {
        response.status(statusCode);
        response.type("application/json; charset=utf-8");
        // Convert Strings to a Map to yield meaningful JSON.
        if (object instanceof String) {
            object = ImmutableMap.of("message", object);
        }
        // Unfortunately we can't just call response.body(jsonMapper.writeValueAsBytes(object));
        // because that only accepts a String parameter.
        // We could call response.body(jsonMapper.writeValueAsBytes(object));
        // but then the calling handler functions need to explicitly return null which is weird.
        try {
            return jsonMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            // The server should catch this and turn it into a 500 response.
            response.status(HttpStatus.SERVER_ERROR_500);
            throw new RuntimeException(e);
        }
    }

    /**
     * Fetch status of all unfinished jobs as a JSON list.
     */
    private String getAllJobs(Request request, Response response) {
        return jsonResponse(response, HttpStatus.OK_200, broker.getJobSummary());
    }

    /**
     * Report all workers that have recently contacted the broker as JSON list.
     */
    private String getAllWorkers(Request request, Response response) {
        return jsonResponse(response, HttpStatus.OK_200, broker.getWorkerObservations());
    }

    /**
     * Workers use this endpoint to fetch tasks from job queues. At the same time, they also report their version
     * information, unique ID, loaded networks, etc. as JSON in the request body. They also supply the results of any
     * completed work via this same object. The broker should preferentially send them work they can do efficiently
     * using already loaded networks and scenarios. The method is POST because unlike GETs (which fetch status) it
     * modifies the contents of the task queue.
     */
    private Object workerPoll (Request request, Response response) {
        WorkerStatus workerStatus = objectFromRequestBody(request, WorkerStatus.class);
        // Record any regional analysis results that were supplied by the worker and mark them completed.
        for (RegionalWorkResult workResult : workerStatus.results) {
            // TODO merge these two methods on the broker?
            broker.handleRegionalWorkResult(workResult);
            broker.markTaskCompleted(workResult);
        }
        // Clear out the results field so it's not visible in the worker list API endpoint.
        workerStatus.results = null;
        // Add this worker to our catalog, tracking its graph affinity and the last time it was seen among other things.
        broker.recordWorkerObservation(workerStatus);
        WorkerCategory workerCategory = workerStatus.getWorkerCategory();
        // See if any appropriate tasks exist for this worker.
        List<RegionalTask> tasks = broker.getSomeWork(workerCategory);
        // If there is no work for the worker, signal this clearly with a "no content" code,
        // so the worker can sleep a while before the next polling attempt.
        if (tasks.isEmpty()) {
            return jsonResponse(response, HttpStatus.NO_CONTENT_204, tasks);
        } else {
            return jsonResponse(response, HttpStatus.OK_200, tasks);
        }
    }

    /**
     * Deserializes an object of the given type from JSON in the body of the supplied Spark request.
     */
    public static <T> T objectFromRequestBody (Request request, Class<T> classe) {
        try {
            return JsonUtil.objectMapper.readValue(request.body(), classe);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
