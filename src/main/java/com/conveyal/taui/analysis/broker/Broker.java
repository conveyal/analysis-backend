package com.conveyal.taui.analysis.broker;

import com.amazonaws.regions.*;
import com.amazonaws.regions.Region;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.conveyal.r5.analyst.WorkerCategory;
import com.conveyal.r5.analyst.cluster.GridResultAssembler;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.cluster.RegionalWorkResult;
import com.conveyal.r5.analyst.cluster.WorkerStatus;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.analysis.RegionalAnalysisStatus;
import com.conveyal.taui.controllers.RegionalAnalysisController;
import com.conveyal.taui.models.RegionalAnalysis;
import com.google.common.io.ByteStreams;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * This class distributes the tasks making up regional jobs to workers.
 *
 * It should aim to draw tasks fairly from all organizations, and fairly from all jobs within each organization,
 * while attempting to respect the transport network affinity of each worker, giving the worker tasks that require
 * the same network it has been using recently.
 *
 * Previously workers long-polled for work, holding lots of connections open. Now they short-poll and sleep for a while
 * if there's no work. This is simpler and allows us to work withing much more standard HTTP frameworks.
 *
 * The fact that workers continuously re-poll for work every 10-30 seconds serves as a signal to the broker that
 * they are still alive and waiting. This also allows the broker to maintain a catalog of active workers.
 *
 * Because (at least currently) two organizations never share the same graph, we can get by with pulling tasks
 * cyclically or randomly from all the jobs, and actively shape the number of workers with affinity for each graph by
 * forcing some of them to accept tasks on graphs other than the one they have declared affinity for.
 *
 * This could be thought of as "affinity homeostasis". We  will constantly keep track of the ideal proportion of workers
 * by graph (based on active jobs), and the true proportion of consumers by graph (based on incoming polling), then
 * we can decide when a worker's graph affinity should be ignored and what it should be forced to.
 *
 * It may also be helpful to mark jobs every time they are skipped in the LRU queue. Each time a job is serviced,
 * it is taken out of the queue and put at its end. Jobs that have not been serviced float to the top.
 *
 * Most methods on this class are synchronized, because they can be called from many HTTP handler threads at once.
 * TODO evaluate whether synchronizing all the functions to make this threadsafe is a performance issue.
 */
public class Broker {

    private static final Logger LOG = LoggerFactory.getLogger(Broker.class);

    // TODO replace with Multimap
    public final CircularList<Job> jobs = new CircularList<>();

    /** The most tasks to deliver to a worker at a time. */
    public final int MAX_TASKS_PER_WORKER = 16;

    /**
     * How long to give workers to start up (in ms) before assuming that they have started (and starting more
     * on a given graph if they haven't.
     */
    public static final long WORKER_STARTUP_TIME = 60 * 60 * 1000;

    /** Maximum number of workers allowed */
    private int maxWorkers;

    /** The configuration that will be applied to workers launched by this broker. */
    private Properties workerConfig;

    /** Keeps track of all the workers that have contacted this broker recently asking for work. */
    protected WorkerCatalog workerCatalog = new WorkerCatalog();

    /** If true, avoid using remote hosted services. */
    private boolean workOffline;

    /** Amazon AWS SDK client. */
    private AmazonEC2 ec2;

    /** These objects piece together results received from workers into one regional analysis result file per job. */
    private static Map<String, GridResultAssembler> resultAssemblers = new HashMap<>();

    /**
     * keep track of which graphs we have launched workers on and how long ago we launched them,
     * so that we don't re-request workers which have been requested.
     */
    public TObjectLongMap<WorkerCategory> recentlyRequestedWorkers = new TObjectLongHashMap<>();

    public Broker () {
        // print out date on startup so that CloudWatch logs has a unique fingerprint
        LOG.info("Analyst broker starting at {}", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));

        this.workOffline = AnalysisServerConfig.offline;

        if (!workOffline) {
            // Create a single properties file that will be used as a template for all worker instances.
            workerConfig = new Properties();
            // TODO rename to worker-log-group in worker code
            workerConfig.setProperty("log-group", AnalysisServerConfig.workerLogGroup);
            workerConfig.setProperty("broker-address", AnalysisServerConfig.serverAddress);
            // TODO rename all config fields in config class, worker and backend to be consistent.
            // TODO Maybe just send the entire analyst config to the worker!
            workerConfig.setProperty("broker-port", Integer.toString(AnalysisServerConfig.serverPort));
            workerConfig.setProperty("worker-port", Integer.toString(AnalysisServerConfig.workerPort));
            workerConfig.setProperty("graphs-bucket", AnalysisServerConfig.bundleBucket);
            workerConfig.setProperty("pointsets-bucket", AnalysisServerConfig.gridBucket);
            // Tell the workers to shut themselves down automatically when no longer busy.
            workerConfig.setProperty("auto-shutdown", "true");
        }

        this.maxWorkers = AnalysisServerConfig.maxWorkers;

        ec2 = new AmazonEC2Client();

        // When running on an EC2 instance, default to the AWS region of that instance
        Region region = null;
        if (!workOffline) {
            region = Regions.getCurrentRegion();
        }
        if (region != null) {
            ec2.setRegion(region);
        }
    }

    /**
     * Enqueue a set of tasks for a regional analysis.
     * Only a single task is passed in, which the broker will expand into all the individual tasks for a regional job.
     * We pass in the group and user only to tag any newly created workers. This should probably be done in the caller.
     * TODO push the creation of the TemplateTask down into this method, to avoid last two parameters?
     * TODO make the tags a simple Map from String -> String here and for worker startup.
     */
    public synchronized void enqueueTasksForRegionalJob (RegionalTask templateTask, String accessGroup, String createdBy) {
        LOG.info("Enqueuing tasks for job {} using template task.", templateTask.jobId);
        if (findJob(templateTask.jobId) != null) {
            LOG.error("Someone tried to enqueue job {} but it already exists.", templateTask.jobId);
            throw new RuntimeException("Enqueued duplicate job " + templateTask.jobId);
        }
        Job job = new Job(templateTask);
        jobs.insertAtTail(job);
        // Register the regional job so results received from multiple workers can be assembled into one file.
        resultAssemblers.put(templateTask.jobId, new GridResultAssembler(templateTask, AnalysisServerConfig.resultsBucket));
        if (AnalysisServerConfig.testTaskRedelivery) {
            // This is a fake job for testing, don't confuse the worker startup code below with null graph ID.
            return;
        }
        if (workerCatalog.noWorkersAvailable(job.workerCategory, workOffline)) {
            createWorkersInCategory(job.workerCategory, accessGroup, createdBy);
        } else {
            // Workers exist in this category, clear out any record that we're waiting for one to start up.
            recentlyRequestedWorkers.remove(job.workerCategory);
        }
    }

    /**
     * Create workers for a given job, if need be.
     * Whoa, we're sending requests to EC2 inside a synchronized block that stops the whole broker!
     * FIXME FIXME FIXME! We should make this an asynchronous operation.
     * @param user only used to tag the newly created instance
     * @param group only used to tag the newly created instance
     */
    public synchronized void createWorkersInCategory (WorkerCategory category, String group, String user) {

        String clientToken = UUID.randomUUID().toString().replaceAll("-", "");

        if (workOffline) {
            LOG.info("Work offline enabled, not creating workers for {}", category);
            return;
        }

        if (workerCatalog.totalWorkerCount() >= maxWorkers) {
            LOG.warn("{} workers already started, not starting more; jobs will not complete on {}", maxWorkers, category);
            return;
        }

        // If workers have already been started up, don't repeat the operation.
        if (recentlyRequestedWorkers.containsKey(category)
                && recentlyRequestedWorkers.get(category) >= System.currentTimeMillis() - WORKER_STARTUP_TIME){
            LOG.info("Workers still starting on {}, not starting more", category);
            return;
        }

        // TODO: should we start multiple workers on large jobs?
        int nWorkers = 1;

        // There are no workers on this graph with the right worker commit, start some.
        LOG.info("Starting {} workers as there are none on {}", nWorkers, category);
        RunInstancesRequest req = new RunInstancesRequest();
        req.setImageId(AnalysisServerConfig.workerAmiId);
        req.setInstanceType(AnalysisServerConfig.workerInstanceType);
        req.setSubnetId(AnalysisServerConfig.workerSubnetId);

        // even if we can't get all the workers we want at least get some
        req.setMinCount(1);
        req.setMaxCount(nWorkers);

        // It's fine to just modify the worker config without a protective copy because this method is synchronized.
        workerConfig.setProperty("initial-graph-id", category.graphId);
        // Tell the worker where to get its R5 JAR. This is a Conveyal S3 bucket with HTTP access turned on.
        String workerDownloadUrl = String.format("https://r5-builds.s3.amazonaws.com/%s.jar", category.workerVersion);

        ByteArrayOutputStream cfg = new ByteArrayOutputStream();
        try {
            workerConfig.store(cfg, "Worker config");
            cfg.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Read in the startup script
        // We used to just pass the config to custom AMI, but by constructing a startup script that initializes a stock
        // Amazon Linux AMI, we don't have to worry about maintaining and keeping our AMI up to date. Amazon Linux applies
        // important security updates on startup automatically.
        try {
            String workerConfigString = cfg.toString();
            InputStream scriptIs = Broker.class.getClassLoader().getResourceAsStream("worker.sh");
            ByteArrayOutputStream scriptBaos = new ByteArrayOutputStream();
            ByteStreams.copy(scriptIs, scriptBaos);
            scriptIs.close();
            scriptBaos.close();
            String scriptTemplate = scriptBaos.toString();
            String logGroup = workerConfig.getProperty("log-group");
            String script = MessageFormat.format(scriptTemplate, workerDownloadUrl, logGroup, workerConfigString);
            // Send the config to the new workers as EC2 "user data"
            String userData = new String(Base64.getEncoder().encode(script.getBytes()));
            req.setUserData(userData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Set the IAM profile of the new worker.
        req.setIamInstanceProfile(new IamInstanceProfileSpecification().withArn(AnalysisServerConfig.workerIamRole));

        // Allow us to retry the instance creation request at will. TODO how does this statement accomplish that?
        req.setClientToken(clientToken);

        // Allow the worker machine to shut itself completely off.
        req.setInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate);

        // Tag the new instance so we can identify it in the EC2 console.
        TagSpecification instanceTags = new TagSpecification().withResourceType(ResourceType.Instance).withTags(
                new Tag("Name","Analysis Worker"),
                new Tag("Project", "Analysis"),
                new Tag("networkId", category.graphId),
                new Tag("workerVersion", category.workerVersion),
                new Tag("group", group),
                new Tag("user", user)
        );
        // TODO check and log result of request.
        RunInstancesResult res = ec2.runInstances(req.withTagSpecifications(instanceTags));

        // Record the fact that we've requested this kind of workers so we don't do it repeatedly.
        recentlyRequestedWorkers.put(category, System.currentTimeMillis());
        LOG.info("Requested {} workers for user {} of group {}", nWorkers, user, group);
    }

    /**
     * Attempt to find some tasks that match what a worker is requesting.
     * Always returns a list, which may be empty if there is nothing to deliver.
     */
    public synchronized List<RegionalTask> getSomeWork (WorkerCategory workerCategory) {
        Job job;
        // FIXME use workOffline boolean instead of examining workerVersion
        if (workerCategory.graphId == null || "UNKNOWN".equalsIgnoreCase(workerCategory.workerVersion)) {
            // This worker has no loaded networks or no specified version, get tasks from the first job that has any.
            // FIXME Note that this is ignoring worker version number! This is useful for debugging though.
            job = jobs.advanceToElement(j -> j.hasTasksToDeliver());
        } else {
            // This worker has a preferred network, get tasks from a job on that network.
            job = jobs.advanceToElement(j -> j.workerCategory.equals(workerCategory) && j.hasTasksToDeliver());
        }
        if (job == null) {
            // No matching job was found.
            return Collections.EMPTY_LIST;
        }
        // Return up to N tasks that are waiting to be processed.
        return job.generateSomeTasksToDeliver(MAX_TASKS_PER_WORKER);
    }

    /**
     * Take a normal (non-priority) task out of a job queue, marking it as completed so it will not be re-delivered.
     * The result of the computation is supplied.
     * TODO separate completion out from returning the work product, since they have different synchronization requirements
     * this would also allow returning errors as JSON and the grid result separately.
     * @return whether the task was found and removed.
     */
    public synchronized boolean markTaskCompleted (RegionalWorkResult workResult) {
        String jobId = workResult.jobId;
        int taskId = workResult.taskId;
        Job job = findJob(jobId);
        if (job == null) {
            LOG.error("Could not find a job with ID {} and therefore could not mark the task as completed.", jobId);
            return false;
        }
        if (!job.markTaskCompleted(taskId)) {
            LOG.error("Failed to mark task {} completed on job {}.", taskId, jobId);
        }
        // Once the last task is marked as completed, the job is finished. Purge it from the list to free memory.
        if (job.isComplete()) {
            job.verifyComplete();
            jobs.remove(job);
        }
        return true;
    }

    /** Find the job for the given jobId, returning null if that job does not exist. */
    public Job findJob (String jobId) {
        for (Job job : jobs) {
            if (job.jobId.equals(jobId)) {
                return job;
            }
        }
        return null;
    }

    /**
     * Delete the job with the given ID.
     */
    public synchronized boolean deleteJob (String jobId) {
        // Remove the job from the broker so we stop distributing its tasks to workers.
        Job job = findJob(jobId);
        if (job == null) return false;
        boolean success = jobs.remove(job);
        // Shut down the object used for assembling results, removing its associated temporary disk file.
        // TODO just put the assembler in the Job object
        GridResultAssembler assembler = resultAssemblers.remove(jobId);
        try {
            assembler.terminate();
        } catch (Exception e) {
            LOG.error("Could not terminate grid result assembler, this may waste disk space. Reason: {}", e.toString());
            success = false;
        }
        // TODO where do we delete the regional analysis from Persistence so it doesn't show up in the UI after deletion?
        return success;
    }

    /**
     * Given a worker commit ID and transport network, return the IP or DNS name of a worker that has that software
     * and network already loaded. If none exist, return null and try to start one.
     */
    public synchronized String getWorkerAddress(WorkerCategory workerCategory) {
        if (workOffline) {
            return "localhost";
        }
        // First try to get a worker that's already loaded the right network.
        // This value will be null if no workers exist in this category - caller should attempt to create some.
        String workerAddress = workerCatalog.getSinglePointWorkerAddressForCategory(workerCategory);
        return workerAddress;
    }


    /**
     * Get a collection of all the workers that have recently reported to this broker.
     * The returned objects are designed to be serializable so they can be returned over an HTTP API.
     */
    public Collection<WorkerObservation> getWorkerObservations () {
        return workerCatalog.getAllWorkerObservations();
    }

    /**
     * Get a collection of all unfinished jobs being managed by this broker.
     * The returned objects are designed to be serializable so they can be returned over an HTTP API.
     */
    public Collection<JobStatus> getJobSummary() {
        List<JobStatus> jobStatusList = new ArrayList<>();
        for (Job job : this.jobs) {
            jobStatusList.add(new JobStatus(job));
        }
        // Add a summary of all jobs to the list.
        jobStatusList.add(new JobStatus(jobStatusList));
        return jobStatusList;
    }

    /**
     * Record information that a worker sent about itself.
     */
    public void recordWorkerObservation(WorkerStatus workerStatus) {
        workerCatalog.catalog(workerStatus);
    }

    /**
     * Slots a single regional work result received from a worker into the appropriate position in the appropriate file.
     * @param workResult an object representing accessibility results for a single-origin, sent by a worker.
     */
    public void handleRegionalWorkResult (RegionalWorkResult workResult) {
        GridResultAssembler assembler = resultAssemblers.get(workResult.jobId);
        if (assembler == null) {
            LOG.error("Received result for unrecognized job ID {}, discarding.", workResult.jobId);
        } else {
            assembler.handleMessage(workResult);
        }
    }

    /**
     * Returns a simple status object intended to inform the UI of job progress.
     */
    public RegionalAnalysisStatus getJobStatus (String jobId) {
        GridResultAssembler gridResultAssembler = resultAssemblers.get(jobId);
        if (gridResultAssembler == null) {
            return null;
        } else {
            return new RegionalAnalysisStatus(gridResultAssembler);
        }
    }

    public boolean anyJobsActive () {
        for (Job job : jobs) {
            if (!job.isComplete()) return true;
        }
        return false;
    }

    public void logJobStatus() {
        for (Job job : jobs) {
            LOG.info(job.toString());
        }
    }
}
