package com.conveyal.taui.models;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import org.bson.types.ObjectId;

import java.io.File;
import java.net.URL;
import java.util.Date;

public class Resource extends Model {
    private static final int REQUEST_TIMEOUT_MSEC = 3600 * 1000;

    public ObjectId regionId;

    // Original filename
    public String filename;

    // Uploaded file content type
    public ContentType contentType;
    public enum ContentType {
        CSV,
        GEOJSON,
        PBF,
        SHAPEFILE,
        ZIP
    }

    public String getPath () {
        String appendedFilename = String.join("-", this._id, this.filename);
        return String.join("/", this.accessGroup, this.regionId.toString(), appendedFilename);
    }

    public File getFile (String basePath) {
        return new File(String.join(basePath, this.getPath()));
    }

    public URL getUploadURL (AmazonS3 s3, String basePath) {
        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(basePath, this.getPath());

        // Allow POST
        presigned.setMethod(HttpMethod.POST);

        // Expire in an hour
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);
        presigned.setExpiration(expiration);
        return s3.generatePresignedUrl(presigned);
    }

    public URL getDownloadURL (AmazonS3 s3, String basePath) {
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(basePath, this.getPath());
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);
        return s3.generatePresignedUrl(presigned);
    }
}
