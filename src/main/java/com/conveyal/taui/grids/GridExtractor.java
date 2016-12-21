package com.conveyal.taui.grids;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * Abstract base class for classes that create opportunity density grids for accessibility analysis
 * from some other data source. These grids are uploaded to S3 for later use once they are created.
 */
public abstract class GridExtractor {

    // A pool of threads that upload newly created grids to S3. The pool is shared between all GridFetchers.
    // The default policy when the pool's work queue is full is to abort with an exception.
    // We shouldn't use the caller-runs policy because that will cause deadlocks.
    private static ThreadPoolExecutor s3Upload = new ThreadPoolExecutor(4, 8, 90, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024));

    private static AmazonS3 s3 = new AmazonS3Client();

    /** Human readable name for this source of grids. */
    public String name;

    /** Prepare an outputstream on S3, set up to gzip and upload whatever is uploaded in a thread. */
    public static OutputStream getOutputStream (String bucket, String key) {
        try {
            PipedOutputStream outputStream = new PipedOutputStream();
            PipedInputStream inputStream = new PipedInputStream(outputStream);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("application/octet-stream");
            metadata.setContentEncoding("gzip");
            PutObjectRequest request = new PutObjectRequest(bucket, key, inputStream, metadata);

            // upload to s3 in a separate thread so that we don't deadlock
            s3Upload.execute(() -> s3.putObject(request));

            return new GZIPOutputStream(outputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
