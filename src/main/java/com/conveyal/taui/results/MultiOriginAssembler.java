package com.conveyal.taui.results;

import com.conveyal.r5.OneOriginContainer;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
import com.conveyal.r5.analyst.cluster.CombinedWorkResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * Assemble results arriving from workers into single combined files per regional analysis on the backend.
 *
 * During distributed computation of access to destinations, workers return raw results for single
 * origins to the broker while polling. These results contain one accessibility measurement per origin grid cell.
 * This class assembles raw results into a combined file.
 */
public class MultiOriginAssembler {

    public static final Logger LOG = LoggerFactory.getLogger(MultiOriginAssembler.class);

    public final AnalysisTask request;

    File bufferFile;

    RandomAccessFile randomAccessFile;

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

    /** The bucket on S3 to which the final result will be written. */
    public final String outputBucket;

    public MultiOriginAssembler(AnalysisTask request, String outputBucket, int nTotal) {
        this.request = request;
        this.outputBucket = outputBucket;
        this.nTotal = nTotal;
        originsReceived = new BitSet(nTotal);
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

    // TODO implement sensible default behavior; overriden in subclasses for now
    public void handleMessage (CombinedWorkResult workResult) {

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

}
