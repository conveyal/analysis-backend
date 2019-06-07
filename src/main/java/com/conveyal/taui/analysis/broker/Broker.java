package com.conveyal.taui.analysis.broker;

import com.conveyal.r5.analyst.WorkerCategory;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.cluster.RegionalWorkResult;
import com.conveyal.r5.analyst.cluster.WorkerStatus;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.GridResultAssembler;
import com.conveyal.taui.analysis.RegionalAnalysisStatus;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import gnu.trove.TCollections;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


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

    public final ListMultimap<WorkerCategory, Job> jobs = MultimapBuilder.hashKeys().arrayListValues().build();

    /** The most tasks to deliver to a worker at a time. */
    public final int MAX_TASKS_PER_WORKER = 16;

    /** Used when auto-starting spot instances. Set to a smaller value to increase the number of workers requested
     * automatically*/
    public final int TARGET_TASKS_PER_WORKER = 800;

    /** We want to request spot instances to "boost" regional analyses after a few regional task results are received
     * for a given workerCategory. Do so after receiving results for an arbitrary task toward the beginning of the job*/
    public final int AUTO_START_SPOT_INSTANCES_AT_TASK  = MAX_TASKS_PER_WORKER * 2 + 10; //42

    /** The maximum number of spot instances allowable in an automatic request */
    public final int MAX_WORKERS_PER_CATEGORY = 250;

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
    private EC2Launcher launcher;

    /** These objects piece together results received from workers into one regional analysis result file per job. */
    private static Map<String, GridResultAssembler> resultAssemblers = new HashMap<>();

    /**
     * keep track of which graphs we have launched workers on and how long ago we launched them,
     * so that we don't re-request workers which have been requested.
     */
    public TObjectLongMap<WorkerCategory> recentlyRequestedWorkers = TCollections.synchronizedMap(new
            TObjectLongHashMap<>());

    public Broker () {
        // print out date on startup so that CloudWatch logs has a unique fingerprint
        LOG.info("Analyst broker starting at {}", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));

        this.workOffline = AnalysisServerConfig.offline;

        if (!workOffline){
            this.launcher = new EC2Launcher();
        }

        this.maxWorkers = AnalysisServerConfig.maxWorkers;

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
        Job job = new Job(templateTask, accessGroup, createdBy);
        jobs.put(job.workerCategory, job);
        // Register the regional job so results received from multiple workers can be assembled into one file.
        resultAssemblers.put(templateTask.jobId, new GridResultAssembler(templateTask, AnalysisServerConfig.resultsBucket));
        if (AnalysisServerConfig.testTaskRedelivery) {
            // This is a fake job for testing, don't confuse the worker startup code below with null graph ID.
            return;
        }
        if (workerCatalog.noWorkersAvailable(job.workerCategory, workOffline)) {
            createOnDemandWorkerInCategory(job.workerCategory, accessGroup, createdBy);
        } else {
            // Workers exist in this category, clear out any record that we're waiting for one to start up.
            recentlyRequestedWorkers.remove(job.workerCategory);
        }
    }

    /**
     * Create on-demand worker for a given job.
     * @param user only used to tag the newly created instance
     * @param group only used to tag the newly created instance
     */
    public void createOnDemandWorkerInCategory(WorkerCategory category, String group, String user){
        createWorkersInCategory(category, group, user, 1, 0);
    }

    /**
     * Create on-demand/spot workers for a given job, after certain checks
     * @param user only used to tag the newly created instance
     * @param group only used to tag the newly created instance
     * @param nOnDemand EC2 on-demand instances to request
     * @param nSpot EC2 spot instances to request
     */

    public void createWorkersInCategory (WorkerCategory category, String group, String user, int
            nOnDemand, int nSpot) {

        if (workOffline) {
            LOG.info("Work offline enabled, not creating workers for {}", category);
            return;
        }

        if (nOnDemand < 0 || nSpot < 0){
            LOG.info("Negative number of workers requested, not starting any");
            return;
        }

        if (workerCatalog.totalWorkerCount() + nOnDemand + nSpot >= maxWorkers) {
            throw AnalysisServerException.forbidden("\"Maximum of {} workers already started, not starting more; jobs" +
                    " will not complete on {}\", maxWorkers, category");
        }

        // If workers have already been started up, don't repeat the operation.
        if (recentlyRequestedWorkers.containsKey(category)
                && recentlyRequestedWorkers.get(category) >= System.currentTimeMillis() - WORKER_STARTUP_TIME){
            LOG.info("Workers still starting on {}, not starting more", category);
            return;
        }

        EC2RequestConfiguration config = new EC2RequestConfiguration(category, group, user);

        launcher.launch(config, nOnDemand, nSpot);

        // Record the fact that we've requested an on-demand worker so we don't do it repeatedly.
        if (nOnDemand > 0) {
            recentlyRequestedWorkers.put(category, System.currentTimeMillis());
        }
        LOG.info("Requested {} on-demand and {} spot workers on {}", nOnDemand, nSpot, config);
    }

    /**
     * Attempt to find some tasks that match what a worker is requesting.
     * Always returns a list, which may be empty if there is nothing to deliver.
     */
    public synchronized List<RegionalTask> getSomeWork (WorkerCategory workerCategory) {
        Job job;
        if (AnalysisServerConfig.offline) {
            // Working in offline mode; get tasks from the first job that has any tasks to deliver.
            job = jobs.values().stream()
                    .filter(j -> j.hasTasksToDeliver()).findFirst().orElse(null);
        } else {
            // This worker has a preferred network, get tasks from a job on that network.
            job = jobs.get(workerCategory).stream()
                    .filter(j -> j.hasTasksToDeliver()).findFirst().orElse(null);
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
            jobs.remove(job.workerCategory, job);
            // This method is called after the regional work results are handled, finishing and closing the local file.
            // So we can harmlessly remove the GridResultAssembler now that the job is removed.
            resultAssemblers.remove(jobId);
        }
        return true;
    }

    /** Find the job for the given jobId, returning null if that job does not exist. */
    public Job findJob (String jobId) {
        return jobs.values().stream().filter(job -> job.jobId.equals(jobId)).findFirst().orElse(null);
    }

    /**
     * Delete the job with the given ID.
     */
    public synchronized boolean deleteJob (String jobId) {
        // Remove the job from the broker so we stop distributing its tasks to workers.
        Job job = findJob(jobId);
        if (job == null) return false;
        boolean success = jobs.remove(job.workerCategory, job);
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
        for (Job job : this.jobs.values()) {
            jobStatusList.add(new JobStatus(job));
        }
        // Add a summary of all jobs to the list.
        jobStatusList.add(new JobStatus(jobStatusList));
        return jobStatusList;
    }

    public synchronized void unregisterSinglePointWorker (WorkerCategory category) {
        workerCatalog.tryToReassignSinglePointWork(category);
    }

    /**
     * Record information that a worker sent about itself.
     */
    public void recordWorkerObservation(WorkerStatus workerStatus) {
        workerCatalog.catalog(workerStatus);
    }

    /**
     * Slots a single regional work result received from a worker into the appropriate position in the appropriate file.
     * Also considers requesting extra spot instances after a few results have been received. The checks in place
     * should prevent an unduly large number of workers from proliferating, assuming jobs for a given worker category (transport
     * network + R5 version) are completed sequentially.
     * @param workResult an object representing accessibility results for a single-origin, sent by a worker.
     */
    public void handleRegionalWorkResult (RegionalWorkResult workResult) {
        GridResultAssembler assembler = resultAssemblers.get(workResult.jobId);
        if (assembler == null) {
            LOG.error("Received result for unrecognized job ID {}, discarding.", workResult.jobId);
        } else {
            assembler.handleMessage(workResult);
            // When results for the task with the magic number are received, consider boosting the job by starting EC2
            // spot instances
            if (workResult.taskId == AUTO_START_SPOT_INSTANCES_AT_TASK) {
                requestExtraWorkersIfAppropriate(workResult);
            }
        }
    }

    private void requestExtraWorkersIfAppropriate(RegionalWorkResult workResult) {
        Job job = findJob(workResult.jobId);
        WorkerCategory workerCategory = job.workerCategory;
        int categoryWorkersAlreadyRunning = workerCatalog.countWorkersInCategory(workerCategory);
        if (categoryWorkersAlreadyRunning < MAX_WORKERS_PER_CATEGORY) {
            // Start a number of workers that scales with the number of total tasks, up to a fixed number.
            // TODO more refined determination of number of workers to start (e.g. using tasks per minute)
            int nSpot = Math.min(MAX_WORKERS_PER_CATEGORY, job.nTasksTotal / TARGET_TASKS_PER_WORKER) -
                    categoryWorkersAlreadyRunning;
            createWorkersInCategory(job.workerCategory, job.accessGroup, job.createdBy, 0, nSpot);
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

    public File getPartialRegionalAnalysisResults (String jobId) {
        GridResultAssembler gridResultAssembler = resultAssemblers.get(jobId);
        if (gridResultAssembler == null) {
            return null;
        } else {
            return gridResultAssembler.getBufferFile();
        }
    }

    public boolean anyJobsActive () {
        for (Job job : jobs.values()) {
            if (!job.isComplete()) return true;
        }
        return false;
    }

    public void logJobStatus() {
        for (Job job : jobs.values()) {
            LOG.info(job.toString());
        }
    }
}
