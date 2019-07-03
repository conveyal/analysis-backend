package com.conveyal.taui.analysis.broker;

import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.WorkerCategory;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * A Job is a collection of tasks that represent all the origins in a regional analysis. All the tasks must have the
 * same network ID and be run against the same R5 version on the workers.
 *
 * There is no concern about multiple tasks having the same ID, because those IDs are created by the broker.
 * An unlikely potential problem is that if the broker restarts, the workers might mark the wrong tasks as completed.
 */
public class Job {

    private static final Logger LOG = LoggerFactory.getLogger(Job.class);

    // TODO Reimplement re-delivery - jobs can get stuck with a few undelivered tasks if something happens to a worker.

    public static final int REDELIVERY_WAIT_SEC = 2 * 60;

    public static final int MAX_DELIVERY_PASSES = 5;

    // In order to provide realistic estimates of job processing time, we don't want to deliver the tasks to
    // workers in row-by-row geographic order, because spatial patterns exist in the world that make some areas
    // much faster than others. Ideally rather than storing the entire sequence (which is O(n) in the number of
    // tasks) you'd want to store a permutation seed that would allow instant lookup of the task in position N.
    // Essentially what we want to do is random selection (sampling) without replacement.
    // As far as I know there's no way to do this without storing the full sequence, or taking longer and longer
    // to find random tasks as the set of completed tasks gets larger.
    // On the other hand, working on tasks from the same geographic area might be more efficient because
    // they probably use all the same transit lines and roads, which will already be in cache.
    // So let's just keep track of where we're at in the sequence.
    private int nextTaskToDeliver;

    /* A unique identifier for this job, we use random UUIDs. */
    public final String jobId;

    // Tags for starting worker
    // FIXME these fields should be grouped together a new type WorkerInstanceTags. They are being stored and passed around a lot.
    // This type should explain these are strings purely for categorization of workers and should not be used for other purposes.
    public final String accessGroup;
    public final String createdBy;
    public final String projectId;
    public final String regionId;

    // This can be derived from other fields but is provided as a convenience.
    public final int nTasksTotal;

    // Each task will be checked off when it a result is returned by the worker.
    // Once the worker has returned a result, the task will never be redelivered.
    private final BitSet completedTasks;

    // The number of remaining tasks can be derived from the deliveredTasks BitSet,
    // but as an optimization we keep a separate counter to avoid constantly scanning over that whole bitset.
    protected int nTasksCompleted;

    // The total number of task deliveries that have occurred. A task may be counted more than once if it is redelivered.
    protected int nTasksDelivered;

    // Every task in this job will be based on this template task, but have its origin coordinates changed.
    private final RegionalTask templateTask;

    // This will serve as as a source of coordinates for each numbered task in the job - one per pointSet point.
    // We will eventually want to expand this to work with any PointSet of origins, not just a grid.
//    private final WebMercatorGridPointSet originGrid;

    /**
     * The only thing that changes from one task to the next is the origin coordinates.
     * @param taskNumber the task number within the job, equal to the point number within the origin point set.
     */
    private RegionalTask makeOneTask (int taskNumber) {
        RegionalTask task = templateTask.clone();
        // We want to support any Pointset but for now we only have grids tied to the task itself.
        // In the future we'll set origin coords from a PointSet object.
        task.x = taskNumber % templateTask.width;
        task.y = taskNumber / templateTask.width;
        task.taskId = taskNumber;
        task.fromLat = Grid.pixelToCenterLat(task.north + task.y, task.zoom);
        task.fromLon = Grid.pixelToCenterLon(task.west + task.x, task.zoom);
        return task;
    }

    /**
     * The graph and r5 commit on which tasks are to be run.
     * All tasks contained in a job must run on the same graph and r5 commit.
     * TODO this field is kind of redundant - it's implied by the template request.
     */
    public final WorkerCategory workerCategory;

    // The last time a non-empty set of tasks was delivered to a worker, in milliseconds since the epoch.
    // Enables a quiet period after all tasks have been delivered, before we attempt any re-delivery.
    long lastDeliveryTime = 0;

    // How many times we have started over delivering tasks, working through those that were not marked complete.
    public int deliveryPass = 0;

    public Job (RegionalTask templateTask, String accessGroup, String createdBy, String projectId, String regionId) {
        this.jobId = templateTask.jobId;
        this.templateTask = templateTask;
        this.nTasksTotal = templateTask.width * templateTask.height;
        this.completedTasks = new BitSet(nTasksTotal);
        this.workerCategory = new WorkerCategory(templateTask.graphId, templateTask.workerVersion);
        this.nTasksCompleted = 0;
        this.nextTaskToDeliver = 0;
        this.createdBy = createdBy;
        this.accessGroup = accessGroup;
        this.projectId = projectId;
        this.regionId = regionId;
    }

    public boolean markTaskCompleted(int taskId) {
        // Don't allow negative or huge task numbers to avoid exceptions or expanding the bitset to a huge size.
        if (taskId < 0 || taskId > nTasksTotal) {
            return false;
        }
        if (completedTasks.get(taskId)) {
            return false;
        } else {
            completedTasks.set(taskId);
            nTasksCompleted += 1;
            return true;
        }
    }

    public boolean isComplete() {
        return nTasksCompleted == nTasksTotal;
    }

    /**
     * @param maxTasks the maximum number of tasks to return.
     * @return some tasks that are not yet marked as completed and have not yet been delivered in this delivery pass.
     */
    public List<RegionalTask> generateSomeTasksToDeliver (int maxTasks) {
        List<RegionalTask> tasks = new ArrayList<>(maxTasks);
        // TODO use special bitset iteration syntax.
        while (nextTaskToDeliver < nTasksTotal && tasks.size() < maxTasks) {
            if (!completedTasks.get(nextTaskToDeliver)) {
                tasks.add(makeOneTask(nextTaskToDeliver));
            }
            nextTaskToDeliver += 1;
        }
        if (!tasks.isEmpty()) {
            this.lastDeliveryTime = System.currentTimeMillis();
        }
        nTasksDelivered += tasks.size();
        return tasks;
    }

    public boolean hasTasksToDeliver() {
        if (this.isComplete()) {
            return false;
        }
        if (nextTaskToDeliver < nTasksTotal) {
            return true;
        }
        // Check whether we should start redelivering tasks - this will be triggered by workers polling.
        // The method that generates more tasks to deliver knows to skip already completed tasks.
        if (System.currentTimeMillis() >= lastDeliveryTime + (REDELIVERY_WAIT_SEC * 1000)) {
            if (deliveryPass >= MAX_DELIVERY_PASSES) {
                LOG.error("Job {} has been delivered {} times and it's still not finished. Not redelivering.", jobId, deliveryPass);
                return false;
            }
            nextTaskToDeliver = 0;
            deliveryPass += 1;
            LOG.warn("Delivered all tasks for job {}, but {} seconds later {} results have not been received. Starting redelivery pass {}.",
                    jobId, REDELIVERY_WAIT_SEC, nTasksTotal - nTasksCompleted, deliveryPass);
            return true;
        }
        return false;
    }

    /**
     * Just as a failsafe, when our counter indicates that the job is complete, actually check how many bits are set.
     */
    public void verifyComplete() {
        if (this.isComplete() && completedTasks.cardinality() != nTasksTotal) {
            LOG.error("Something is amiss in completed task tracking.");
        }
    }

    @Override
    public String toString() {
        return "Job{" +
                "jobId='" + jobId + '\'' +
                ", nTasksTotal=" + nTasksTotal +
                ", nTasksCompleted=" + nTasksCompleted +
                ", deliveryPass=" + deliveryPass +
                '}';
    }
}
