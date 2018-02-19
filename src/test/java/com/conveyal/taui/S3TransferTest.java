package com.conveyal.taui;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.io.ByteStreams;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;

/**
 * Test some theories about slow transfers to/from S3. This is not an automated unit test, it's a manually run
 * performance indicator.
 *
 * We were thinking that non-buffered writes or reads were slowing things down, but buffering does not seem to
 * make any difference. It could also have something to do with S3 bucket location, but the slowdown is observed even
 * in very local S3 buckets. The only thing that seems to help is using the s3 TransferManager but speed is still a bit
 * asymmetric.
 */
public class S3TransferTest {

    private static final boolean BUFFER_STREAMS = false;

    private static long beginTime;

    private static final AmazonS3 s3 = new AmazonS3Client();

    private static final TransferManager s3TransferManager = TransferManagerBuilder.defaultTransferManager();

    private static String testFile = "/Users/abyrd/geodata/nl_zh_network_inputs.zip";

    private static String bucket = "conveyal-tmp";

    private static boolean useTransferManager = false;

    public static void main (String[] args) throws Exception {
        if (args.length == 3) {
            System.out.println("Using command line arguments: " + Arrays.toString(args));
            testFile = args[0];
            bucket = args[1];
            useTransferManager = "true".equalsIgnoreCase(args[2]);
        } else {
            System.out.println("Using defaults.");
        }
        for (int i = 0; i < 10; i++) {
            String key = uploadToS3();
            downloadFromS3(key);
        }
    }

    private static String uploadToS3 () throws Exception {
        String key = UUID.randomUUID().toString();
        File file = new File(testFile);
        timeBegin("upload");
        if (useTransferManager) {
            Upload upload = s3TransferManager.upload(bucket, key, file);
            upload.waitForCompletion();
        } else {
            s3.putObject(bucket, key, file);
        }
        timeEnd(file.length());
        return key;
    }

    private static void downloadFromS3 (String key) throws Exception {
        S3Object s3Object = s3.getObject(bucket, key);
        InputStream s3inputStream = s3Object.getObjectContent();
        File tempFile = File.createTempFile("s3test", "tmp");
        timeBegin("download");
        if (useTransferManager) {
            Download download = s3TransferManager.download(bucket, key, tempFile);
            download.waitForCompletion();
        } else {
            OutputStream fileOutputStream = new FileOutputStream(tempFile);
            if (BUFFER_STREAMS) {
                s3inputStream = new BufferedInputStream(s3inputStream);
                fileOutputStream = new BufferedOutputStream(fileOutputStream);
            }
            ByteStreams.copy(s3inputStream, fileOutputStream);
            s3inputStream.close();
            fileOutputStream.close();
        }
        timeEnd(tempFile.length());
    }

    private static void timeBegin (String action) {
        System.out.println("Beginning action: " + action);
        beginTime = System.currentTimeMillis();
    }

    private static double timeEnd (long fileSizeBytes) {
        long endTime = System.currentTimeMillis();
        double elapsedTime = (endTime - beginTime) / 1000D;
        System.out.println("Elapsed time (sec): " + elapsedTime);
        System.out.println("Average speed (MiB/sec): " + (fileSizeBytes / 1024D / 1024D / elapsedTime));
        beginTime = 0;
        return elapsedTime;
    }

}
