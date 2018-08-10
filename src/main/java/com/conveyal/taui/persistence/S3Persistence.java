package com.conveyal.taui.persistence;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.conveyal.taui.util.JsonUtil;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

public class S3Persistence extends FilePersistence {
    private final int REQUEST_TIMEOUT_MSEC = 300 * 1000;
    private final AmazonS3 s3;

    public S3Persistence(String awsRegion) {
        s3 = AmazonS3ClientBuilder.standard().withRegion(awsRegion).build();
    }

    public File create(String directory, String path) throws IOException {
        return File.createTempFile(String.format("%s/%s", directory, path), "tmp");
    }

    public boolean exists(String directory, String path) {
        return s3.doesObjectExist(directory, path);
    }

    public Object put(String directory, String path, Object data) throws IOException {
        File temp = create(directory, path);
        JsonUtil.objectMapper.writeValue(temp, data);
        PutObjectResult por = s3.putObject(directory, path, temp);
        temp.delete();
        return por;
    }

    public Object put(String directory, String path, File file) {
        return s3.putObject(directory, path, file);
    }

    public Object put(String directory, String path, File file, ObjectMetadata metadata) {
        return s3.putObject(new PutObjectRequest(directory, path, file).withMetadata(metadata));
    }

    public InputStream get(String directory, String path) {
        return s3.getObject(directory, path).getObjectContent();
    }

    public boolean delete(String directory, String path) {
        s3.deleteObject(directory, path);
        return s3.doesObjectExist(directory, path);
    }

    public Object respondWithRedirectOrFile(Response response, String directory, String path) {
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        response.type("text/plain");
        return s3.generatePresignedUrl(directory, path, expiration, HttpMethod.GET).toString();
    }
}
