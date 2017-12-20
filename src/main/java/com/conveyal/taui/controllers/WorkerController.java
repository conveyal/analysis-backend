package com.conveyal.taui.controllers;

import com.conveyal.r5.analyst.broker.Broker;
import com.conveyal.r5.analyst.broker.WorkerCategory;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.cluster.TravelTimeSurfaceTask;
import com.conveyal.r5.analyst.cluster.WorkerStatus;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.analysis.RegionalAnalysisManager;
import com.conveyal.taui.models.AnalysisRequest;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.Region;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.HttpStatus;
import com.conveyal.taui.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static spark.Spark.get;
import static spark.Spark.head;
import static spark.Spark.post;

/**
 * This is a Spark HTTP controller to handle connections from workers reporting their status and requesting work.
 * This will replace the separate broker process.
 * Created by abyrd on 2017-12-12
 *
 * Workers used to long-poll and hold connections open, allowing them to receive tasks instantly, as soon as the tasks
 * are enqueued. However, this adds a lot of complexity since we need to suspend the open connections and clear them
 * out when the connections are closed. I am now having the workers short-poll, which allows a simple, standard
 * HTTP API. We don't care if it takes a few seconds for workers to start consuming regional tasks.
 *
 * The long polling originally only existed for high-priority tasks and has been extended to single point
 * tasks, which we will instead handle with a proxy-like push approach.
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
        get("/api/jobs", this::getAllJobs);
        get("/api/workers", this::getAllWorkers);
        // add endpoint to delete jobs
        post("/api/enqueue", this::enqueueRegional);
        post("/api/dequeue", this::dequeueRegional);
        post("/api/complete/:jobId/:taskId", this::taskCompleted); // TODO parameter to indicate error/task failure
        post("/api/analysis", this::singlePoint); // TODO rename to "single" or something
    }

    /**
     * Handler for single-origin requests. This endpoint is contacted by the frontend rather than the worker, and
     * causes a single-point task to be pushed to an appropriate worker for immediate processing. These requests
     * typically come from an interactive session where the user is moving the origin point around in the web UI.
     * Unlike regional jobs where workers pull tasks from a queue, single point tasks work like a proxy or load balancer.
     * @return whatever the worker responds, as an input stream. Spark serializer chain can properly handle streams.
     */
    private Object singlePoint(Request request, Response response) {
        // Deserialize the task in the request body so we can see what kind of worker it wants.
        // Perhaps we should allow both travel time surface and accessibility calculation tasks to be done as single points.
        // AnalysisRequest (backend) vs. AnalysisTask (R5)
        // The accessgroup stuff is copypasta from the old single point controller.
        // We already know the user is authenticated, and we need not check if they have access to the graphs etc,
        // as they're all coded with UUIDs which contain significantly more entropy than any human's account password.
        final String accessGroup = request.attribute("accessGroup");
        AnalysisRequest analysisRequest = objectFromRequestBody(request, AnalysisRequest.class);
        Project project = Persistence.projects.findByIdIfPermitted(analysisRequest.projectId, accessGroup);
        // This is where we transform the task into a slightly different type for R5
        TravelTimeSurfaceTask task = (TravelTimeSurfaceTask) analysisRequest.populateTask(new TravelTimeSurfaceTask(), project);
        WorkerCategory workerCategory = task.getWorkerCategory();
        String address = broker.getWorkerAddress(workerCategory);
        if (address == null) {
            // No workers exist. Kick one off and return "service unavailable".
            response.header("Retry-After", "30");
            return jsonResponse(response, HttpStatus.ACCEPTED_202, "Starting workers, try again later.");
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
            response.header("Content-Type", "application/octet-stream");
            response.header("Content-Encoding", "gzip");
            HttpEntity entity = workerResponse.getEntity();
            LOG.info("Returning worker response to UI.");
            return entity.getContent();
        } catch (Exception e) {
            response.status(HttpStatus.SERVER_ERROR_500);
            return "Exception while talking to worker: " + e.toString();
        }
    }


    /**
     * TODO respond to HEAD requests - for some reason we needed to supply this - proxy or cache?
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
     * This endpoint is used by the front end to enqueue a regional job.
     * A single representative task is sent in the request body as JSON.
     * TODO check that reading objects with objectMapper is threadsafe.
     */
    private Object enqueueRegional (Request request, Response response) {
        RegionalTask templateTask = objectFromRequestBody(request, RegionalTask.class);
        broker.enqueueTasksForRegionalJob(templateTask);
        return jsonResponse(response, HttpStatus.ACCEPTED_202, "Accepted regional job.");
    }

    /**
     * Workers use this command to fetch tasks from a work queue.
     * TODO supply any completed work within this same JSON body
     * They also supply their R5 commit, loaded network IDs, a unique worker ID etc. in the request body so the broker
     * can preferentially send them work they can do efficiently (with already loaded networks and other resources).
     * The method is POST because unlike GETs (which fetch status) it modifies the contents of the task queue.
     */
    private Object dequeueRegional (Request request, Response response) {
        WorkerStatus workerStatus = objectFromRequestBody(request, WorkerStatus.class);
        // Assume one loaded graph (or preferred graph at startup) in the current system
        // Add this worker to our catalog, tracking its graph affinity and the last time it was seen.
        broker.recordWorkerObservation(workerStatus);
        WorkerCategory workerCategory = workerStatus.getWorkerCategory();
        // Tell the caller whether any tasks exist.
        List<AnalysisTask> tasks = broker.getSomeWork(workerCategory);
        // If there is no work for the worker. Signal this clearly with a "no content" code,
        // so the worker can sleep a while before the next polling attempt.
        if (tasks.isEmpty()) {
            return jsonResponse(response, HttpStatus.NO_CONTENT_204, tasks);
        }
        return jsonResponse(response, HttpStatus.OK_200, tasks);
    }

    /**
     * Workers use this command to tell the broker they have finished a task.
     * TODO add a way to signal that an error occurred when processing this task.
     * TODO eliminate, supply the work results and any error messages within the JSON when the worker requests more work.
     */
    private Object taskCompleted (Request request, Response response) {
        String jobId = request.params("jobId");
        int taskId = Integer.parseInt(request.params("taskId"));
        byte[] result = request.bodyAsBytes();
        RegionalAnalysisManager.consumer.registerResult(jobId, result);
        // Only this part is synchronized, grid assembler has its own synchronized block.
        broker.markTaskCompleted(jobId, taskId);
        return jsonResponse(response, HttpStatus.OK_200, "Task marked completed.");
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
