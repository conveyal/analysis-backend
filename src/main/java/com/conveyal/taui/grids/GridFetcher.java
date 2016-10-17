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
 * Abstract class to fetch grids.
 */
public abstract class GridFetcher {
    private static ThreadPoolExecutor s3Upload = new ThreadPoolExecutor(4, 8, 90, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024));

    static {
        // can't use caller-runs as that would cause deadlocks
        s3Upload.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
    }

    private static AmazonS3 s3 = new AmazonS3Client();

    /** Human readable name for this source */
    public String name;

    /** Get an outputstream on S3, already set up and gzipped */
    protected static OutputStream getOutputStream (String bucket, String key) {
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
