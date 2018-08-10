package com.conveyal.taui.persistence;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.taui.AnalysisServerConfig;
import spark.Response;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public abstract class FilePersistence {
    public static final FilePersistence in;

    static {
        // Create a single central class for file persistence, either using S3 or using
        if (AnalysisServerConfig.offline) {
            in = new LocalFilePersistence(AnalysisServerConfig.localCacheDirectory);
        } else {
            in = new S3Persistence(AnalysisServerConfig.awsRegion);
        }
    }

    public abstract File create(String directory, String path) throws IOException;
    public abstract boolean exists (String directory, String path);
    public abstract Object put (String directory, String path, Object data) throws IOException;
    public abstract Object put (String directory, String path, File file, ObjectMetadata metadata);
    public abstract InputStream get (String directory, String path) throws FileNotFoundException;
    public abstract boolean delete (String directory, String path);
    public abstract Object respondWithRedirectOrFile (Response response, String directory, String path) throws IOException;
}
