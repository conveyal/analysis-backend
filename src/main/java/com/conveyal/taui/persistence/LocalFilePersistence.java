package com.conveyal.taui.persistence;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.taui.util.JsonUtil;
import spark.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

public class LocalFilePersistence extends FilePersistence {
    private final String baseDirectory;

    private String format (String directory, String path) {
        return String.format("%s/%s/%s", baseDirectory, directory, path);
    }

    public LocalFilePersistence(String directory) {
        baseDirectory = directory;
        File bd = new File(baseDirectory);
        bd.mkdir();
    }

    public File create (String directory, String path) {
        File file = new File(format(directory, path));
        // Ensure the directories have been made
        file.mkdirs();

        return file;
    }

    public boolean exists (String directory, String path) {
        return new File(format(directory, path)).exists();
    }

    public Object put(String directory, String path, Object data) throws IOException {
        File file = create(directory, path);
        JsonUtil.objectMapper.writeValue(file, data);
        return file;
    }

    public Object put(String directory, String path, File file, ObjectMetadata metadata) {
        return file;
    }

    public InputStream get(String directory, String path) throws FileNotFoundException {
        return new FileInputStream(format(directory, path));
    }

    public boolean delete(String directory, String path) {
        File f = new File(format(directory, path));
        return f.delete();
    }

    public Object respondWithRedirectOrFile(Response response, String directory, String path) throws IOException {
        response.type("application/octet-stream");
        Files.copy(new File(format(directory, path)).toPath(), response.raw().getOutputStream());
        return null;
    }
}
