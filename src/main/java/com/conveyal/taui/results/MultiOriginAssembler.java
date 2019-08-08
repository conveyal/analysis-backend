package com.conveyal.taui.results;

import com.conveyal.r5.analyst.FreeFormPointSet;
import com.conveyal.r5.analyst.cluster.CombinedWorkResult;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.analysis.broker.Job;
import com.conveyal.taui.controllers.OpportunityDatasetController;
import com.conveyal.taui.controllers.RegionalAnalysisController;
import com.csvreader.CsvWriter;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.zip.GZIPOutputStream;

import static com.conveyal.r5.common.Util.human;

/**
 * Assemble results arriving from workers into single combined files per regional analysis on the backend.
 *
 */
public class MultiOriginAssembler {

    public static final Logger LOG = LoggerFactory.getLogger(MultiOriginAssembler.class);

    public final Job job;

    File bufferFile;

    private BufferedWriter bufferedWriter;

    private CsvWriter writer;

    private String[] originIds;

    private String[] destinationIds;

    private String suffix = "_times.csv";

    boolean error = false;

    /**
     * The number of results received for unique origin points (i.e. two results for the same origin should only
     * increment this once). It does not need to be an atomic int as it's incremented in a synchronized block.
     */
    public int nComplete = 0;

    // We need to keep track of which specific origins are completed, to avoid double counting if we receive more than
    // one result for the same origin.
    BitSet originsReceived;

    /** Total number of results expected. */
    public int nTotal;

    int nPercentiles = 1;

    /** The bucket on S3 to which the final result will be written. */
    public final String outputBucket;

    public MultiOriginAssembler(Job job, String outputBucket, int nTotal) {
        this.job = job;
        this.outputBucket = outputBucket;
        this.nTotal = nTotal;
        this.originsReceived = new BitSet(nTotal);
    }

    public MultiOriginAssembler(Job job, String outputBucket) {
        this.job = job;
        this.outputBucket = outputBucket;
        this.nPercentiles = job.templateTask.percentiles.length;
        this.nTotal = job.nTasksTotal * job.templateTask.nTravelTimeTargetsPerOrigin;
    }

    public void prepare() {
        if (nTotal > 1_000_000) {
            error = true;
            throw new AnalysisServerException("Temporarily limited to 1 million origin-destination pairs");
        } else {
            LOG.info("Creating file to store results for {} origins.", nTotal);
            try {
                bufferFile = File.createTempFile(job.jobId, suffix);
                // On unexpected server shutdown, these files should be deleted.
                // We could attempt to recover from shutdowns but that will take a lot of changes and persisted data.
                bufferFile.deleteOnExit();

                bufferedWriter = new BufferedWriter(new FileWriter(bufferFile));

                writer = new CsvWriter(bufferedWriter, ',');

                writer.writeRecord(new String[]{"origin", "destination", "time"});

                if (job.originPointSet != null && job.originPointSet instanceof FreeFormPointSet) {
                    originIds = ((FreeFormPointSet) job.originPointSet).ids;
                } if (job.templateTask.destinationPointSetId != null) {
                    destinationIds = OpportunityDatasetController.readFreeForm(job.templateTask.destinationPointSetId,
                            job.accessGroup).ids;
                }

            } catch (Exception e) {
                error = true;
                LOG.error("Exception while creating multi-origin assembler: " + e.toString());
            }
        }
    }

    /**
     * Gzip the access grid and upload it to S3.
     */
    protected synchronized void finish () {
        LOG.info("Finished receiving data for multi-origin analysis {}, uploading to S3", job.jobId);
        try {
            bufferedWriter.flush();
            writer.close();

            File gzippedCsvFile = File.createTempFile(job.jobId, ".access_grid.gz");
            InputStream is = new BufferedInputStream(new FileInputStream(bufferFile));
            OutputStream os = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(gzippedCsvFile)));
            ByteStreams.copy(is, os);
            is.close();
            os.close();

            LOG.info("GZIP compression reduced regional analysis {} from {} to {} ({}x compression)",
                    job.jobId,
                    human(bufferFile.length(), "B"),
                    human(gzippedCsvFile.length(), "B"),
                    (double) bufferFile.length() / gzippedCsvFile.length()
            );
            // TODO use generic filePersistence instead of specific S3 client
            RegionalAnalysisController.s3.putObject(outputBucket, String.format("%s_times.csv", job.jobId),
                    gzippedCsvFile);
            // Clear temporary files off of the disk because the gzipped version is now on S3.
            bufferFile.delete();
            gzippedCsvFile.delete();
        } catch (Exception e) {
            LOG.error("Error uploading results of multi-origin analysis {}", job.jobId, e);
        }
    }

    /**
     * Write to the CSV
     */
    void writeValueAndMarkOriginComplete (int originIndex, String originId, String destinationId, int time) throws IOException {
        // RandomAccessFile is not threadsafe and multiple threads may call this, so the actual writing is synchronized.
        synchronized (this) {
            writer.writeRecord(new String[]{originId, destinationId, String.valueOf(time)});

            if (!originsReceived.get(originIndex)) {
                originsReceived.set(originIndex);
                nComplete += 1;
            }
        }
    }

    public void handleMessage (CombinedWorkResult workResult) {
        try {
            String originId = originIds[workResult.taskId];
            checkDimension(workResult, "percentiles", workResult.travelTimeValues.length, nPercentiles);
            for (int i = 0; i < workResult.travelTimeValues.length; i++) {
                int[] percentileResult = workResult.travelTimeValues[i];
                checkDimension(workResult, "destinations", percentileResult.length, job.templateTask.nTravelTimeTargetsPerOrigin);
                for (int j = 0; j < percentileResult.length; j ++) {
                    String destinationId = destinationIds[j];
                    writeValueAndMarkOriginComplete(workResult.taskId, originId, destinationId, percentileResult[j]);
                }
            }
        } catch (Exception e) {
            error = true;
            LOG.error("Error assembling results for query {}", job.jobId, e);
        }
    }

    /** Clean up and cancel a consumer. */
    public synchronized void terminate () throws IOException {
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
