package com.conveyal.taui.results;

import com.conveyal.r5.analyst.FreeFormPointSet;
import com.conveyal.r5.analyst.cluster.CombinedWorkResult;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.analysis.broker.Job;
import com.csvreader.CsvWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;

import static com.conveyal.r5.common.Util.human;

/**
 * Assemble results arriving from workers into single combined files per regional analysis on the backend.
 *
 */
public class MultiOriginAssembler {

    public static final Logger LOG = LoggerFactory.getLogger(MultiOriginAssembler.class);

    public final Job job;

    File bufferFile;

    RandomAccessFile randomAccessFile;

    String suffix = ".batch_times";

    /** TODO use Java NewIO file channel for native byte order output to our output file. */
//    private FileChannel outputFileChannel;

    boolean error = false;

    /**
     * The number of results received for unique origin points (i.e. two results for the same origin should only
     * increment this once). It does not need to be an atomic int as it's incremented in a synchronized block.
     */
    public int nComplete = 0;

    // We need to keep track of which specific origins are completed, to avoid double counting if we receive more than
    // one result for the same origin.
    private BitSet originsReceived;

    /** Total number of results expected. */
    public int nTotal;

    int nPercentiles = 1;

    /** The bucket on S3 to which the final result will be written. */
    public final String outputBucket;

    public MultiOriginAssembler(Job job, String outputBucket, int nTotal) {
        this.job = job;
        this.outputBucket = outputBucket;
        this.nTotal = nTotal;
        originsReceived = new BitSet(nTotal);
    }

    public MultiOriginAssembler(Job job, String outputBucket) {
        RegionalTask task = job.templateTask;
        this.job = job;
        this.outputBucket = outputBucket;
        this.nPercentiles = job.templateTask.percentiles.length;
        // TODO allow non-square origin-destination skims
        this.nTotal = task.oneToOne ? job.nTasksTotal : job.nTasksTotal * job.nTasksTotal;
    }

    public void prepare() {
        if (nTotal > 1_000_000) {
            error = true;
            throw new AnalysisServerException("Temporarily limited to 1 million origin-destination pairs");
        } else {
            long outputFileSizeBytes = nTotal * job.templateTask.percentiles.length * Integer.BYTES;
            LOG.info("Creating temporary file to store multi-origin results, size is {}.",
                    human(outputFileSizeBytes, "B"));
            try {
                bufferFile = File.createTempFile(job.jobId, suffix);
                // On unexpected server shutdown, these files should be deleted.
                // We could attempt to recover from shutdowns but that will take a lot of changes and persisted data.
                bufferFile.deleteOnExit();

                // We used to fill the file with zeros here, to "overwrite anything that might be in the file already"
                // according to a code comment. However that creates a burst of up to 1GB of disk activity, which exhausts
                // our IOPS budget on cloud servers with network storage. That then causes the server to fall behind in
                // processing incoming results.
                // This is a newly created temp file, so setting it to a larger size should just create a sparse file
                // full of blocks of zeros (at least on Linux, I don't know what it does on Windows).
                this.randomAccessFile = new RandomAccessFile(bufferFile, "rw");
                randomAccessFile.setLength(outputFileSizeBytes);
                LOG.info("Created temporary file of {} to accumulate results from workers.", human(randomAccessFile.length(), "B"));
            } catch (Exception e) {
                error = true;
                LOG.error("Exception while creating regional access grid: " + e.toString());
            }
        }
    }

    /**
     * Gzip the access grid and upload it to S3.
     */
    protected synchronized void finish () {
        LOG.info("Finished receiving data for multi-origin analysis {}, uploading to S3", job.jobId);
        try {
            randomAccessFile.close();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(bufferFile)));

            CsvWriter csvWriter = new CsvWriter(job.jobId + "_times.csv");

            // TODO allow non-square origin-destination skims
            for (int origin = 0; origin < nTotal; origin++) {
                for (int destination = 0; destination < nTotal; destination ++) {
                    String originId = ((FreeFormPointSet) job.originPointSet).ids[origin];
                    String destinationId = ((FreeFormPointSet) job.originPointSet).ids[destination];
                    ArrayList<Integer> times = new ArrayList<>();
                    for (int p = 0; p < nPercentiles; p++) {
                        times.add(reader.read());
                    }
                    csvWriter.writeRecord(new String[]{originId, destinationId, times.toArray().toString()});
                }
            }

            reader.close();
            csvWriter.close();

        } catch (Exception e) {
            LOG.error("Error uploading results of regional analysis {}", job.jobId, e);
        }
    }

    /**
     * TODO is this inefficient? Would it be reasonable to just store the regional results in memory in a byte buffer
     * instead of writing mini byte buffers into files? We should also be able to use a filechannel with native order.
     */
    public static byte[] intToLittleEndianByteArray (int i) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(i);
        return byteBuffer.array();
    }

    /**
     * Write to the proper subregion of the buffer for this origin.
     */
    void writeValueAndMarkOriginComplete (int originIndex, long offset, int value) throws IOException {
        // RandomAccessFile is not threadsafe and multiple threads may call this, so the actual writing is synchronized.
        synchronized (this) {
            randomAccessFile.seek(offset);
            randomAccessFile.write(intToLittleEndianByteArray(value));
            // Don't double-count origins if we receive them more than once.
            if (!originsReceived.get(originIndex)) {
                originsReceived.set(originIndex);
                nComplete += 1;
            }
        }
    }

    public void handleMessage (CombinedWorkResult workResult) {
        try {
            checkDimension(workResult, "percentiles", workResult.travelTimeValues.length, nPercentiles);
            for (int i = 0; i < workResult.travelTimeValues.length; i++) {
                int[] percentileResult = workResult.travelTimeValues[i];
                checkDimension(workResult, "destinations", percentileResult.length,
                        job.templateTask.destinationPointSet.featureCount());
                for (int travelTimeAtPercentile : percentileResult) {
                    long offset = (workResult.taskId + i) * Integer.BYTES;
                    writeValueAndMarkOriginComplete(workResult.taskId, offset, travelTimeAtPercentile);
                }
            }
        } catch (Exception e) {
            error = true;
            LOG.error("Error assembling results for query {}", job.jobId, e);
        }
    }

    /** Clean up and cancel a consumer. */
    public synchronized void terminate () throws IOException {
        this.randomAccessFile.close();
        bufferFile.delete();
    }

    /** This leaks the file object out of the abstraction so is not ideal, but will work for now. */
    public File getBufferFile() {
        return bufferFile;
    }

    void checkDimension (CombinedWorkResult workResult, String dimensionName, int seen, int expected) {
        if (seen != expected) {
            LOG.error("Result for task {} of job {} has {} {}, expected {}.",
                    workResult.taskId, workResult.jobId, dimensionName, seen, expected);
            error = true;
        }
    }

}
