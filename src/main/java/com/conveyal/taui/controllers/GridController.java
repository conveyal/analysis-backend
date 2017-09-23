package com.conveyal.taui.controllers;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.grids.GridExtractor;
import com.conveyal.taui.grids.SeamlessCensusGridExtractor;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.Jobs;
import com.conveyal.taui.util.JsonUtil;
import com.conveyal.taui.util.WrappedURL;
import com.google.common.io.Files;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.conveyal.taui.util.SparkUtil.haltWithJson;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Controller that handles fetching grids.
 */
public class GridController {
    private static final Logger LOG = LoggerFactory.getLogger(GridController.class);

    private static final AmazonS3 s3 = new AmazonS3Client();

    private static final FileItemFactory fileItemFactory = new DiskFileItemFactory();

    /** Store upload status objects */
    private static Map<String, GridUploadStatus> uploadStati = new HashMap<>();

    /** How long request URLs are good for */
    public static final int REQUEST_TIMEOUT_MSEC = 15 * 1000;

    public static Object getGrid (Request req, Response res) {
        // TODO handle offline mode
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        // TODO check project membership

        String key = String.format("%s/%s.grid", req.params("projectId"), req.params("gridId"));

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(AnalysisServerConfig.gridBucket, key);
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);

        URL url = s3.generatePresignedUrl(presigned);

        boolean redirect = true;

        try {
            String redirectParam = req.queryParams("redirect");
            if (redirectParam != null) redirect = Boolean.parseBoolean(redirectParam);
        } catch (Exception e) {
            // do nothing
        }

        if (redirect) {
            res.redirect(url.toString());
            res.status(302); // temporary redirect, this URL will soon expire
            res.type("text/plain"); // override application/json default
            return res;
        } else {
            return new WrappedURL(url);
        }
    }

    public static GridUploadStatus getUploadStatus (Request req, Response res) {
        String handle = req.params("handle");
        // no need for security, the UUID provides plenty
        return uploadStati.get(handle);
    }

    /** Handle many types of file upload. Returns a GridUploadStatus which has a handle to request status. */
    public static GridUploadStatus createGrid (Request req, Response res) throws Exception {
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        Map<String, List<FileItem>> query = sfu.parseParameterMap(req.raw());
        String projectId = req.params("projectId");

        // TODO check project membership

        String dataSet = query.get("Name").get(0).getString("UTF-8");

        GridUploadStatus status = new GridUploadStatus();
        status.status = Status.PROCESSING;
        status.handle = UUID.randomUUID().toString();
        uploadStati.put(status.handle, status);

        Jobs.service.submit(() -> {
            Map<String, Grid> grids = null;

            for (FileItem fi : query.get("files")) {
                String name = fi.getName();
                if (name.endsWith(".csv")) {
                    LOG.info("Detected grid stored as CSV");
                    grids = createGridsFromCsv(query, status);
                    break;
                } else if (name.endsWith(".grid")) {
                    LOG.info("Detected grid stored in Conveyal binary format.");
                    grids = createGridsFromBinaryGridFiles(query, status);
                    break;
                } else if (name.endsWith(".shp")) {
                    LOG.info("Detected grid stored as shapefile");
                    grids = createGridsFromShapefile(query, fi.getName().substring(0, name.length() - 4), status);
                    break;
                }
            }

            if (grids == null) {
                status.status = Status.ERROR;
                return null;
            } else {
                status.status = Status.DONE;
                List<Project.Indicator> indicators = writeGridsToS3(grids, projectId, dataSet);
                Project project = Persistence.projects.get(projectId).clone();
                project.indicators = new ArrayList<>(project.indicators);
                project.indicators.addAll(indicators);
                Persistence.projects.put(projectId, project);
                return indicators;
            }
        });

        return status;
    }

    /** Create a grid from WGS 84 points in a CSV file */
    private static Map<String, Grid> createGridsFromCsv(Map<String, List<FileItem>> query, GridUploadStatus status) throws Exception {
        String latField = query.get("latField").get(0).getString("UTF-8");
        String lonField = query.get("lonField").get(0).getString("UTF-8");

        List<FileItem> file = query.get("files");

        if (file.size() != 1) {
            LOG.warn("CSV upload only supports one file at a time");
            haltWithJson(400, "CSV upload only supports one file at a time.");
        }

        // create a temp file because we have to loop over it twice
        File tempFile = File.createTempFile("grid", ".csv");
        file.get(0).write(tempFile);

        Map<String, Grid> grids = Grid.fromCsv(tempFile, latField, lonField, SeamlessCensusGridExtractor.ZOOM, (complete, total) -> {
            status.completedFeatures = complete;
            status.totalFeatures = total;
        });
        // clean up
        tempFile.delete();

        return grids;
    }

    /**
     * Create a grid from an input stream containing a binary grid file.
     * For those in the know, we can upload manually created binary grid files.
     */
    private static Map<String, Grid> createGridsFromBinaryGridFiles (Map<String, List<FileItem>> query, GridUploadStatus status) throws Exception {
        Map<String, Grid> grids = new HashMap<>();
        List<FileItem> uploadedFiles = query.get("files");
        status.totalFeatures = uploadedFiles.size();
        for (FileItem fileItem : uploadedFiles) {
            Grid grid = Grid.read(fileItem.getInputStream());
            grids.put(fileItem.getName(), grid);
            status.completedFeatures += 1;
        }
        return grids;
    }

    private static Map<String, Grid> createGridsFromShapefile (Map<String, List<FileItem>> query, String baseName, GridUploadStatus status) throws Exception {
        // extract relevant files: .shp, .prj, .dbf, and .shx.
        // We need the SHX even though we're looping over every feature as they might be sparse.
        Map<String, FileItem> filesByName = query.get("files").stream()
                .collect(Collectors.toMap(FileItem::getName, f -> f));

        if (!filesByName.containsKey(baseName + ".shp") ||
                    !filesByName.containsKey(baseName + ".prj") ||
                    !filesByName.containsKey(baseName + ".dbf")) {
            haltWithJson(400, "Shapefile upload must contain .shp, .prj, and .dbf");
        }

        File tempDir = Files.createTempDir();

        File shpFile = new File(tempDir, "grid.shp");
        filesByName.get(baseName + ".shp").write(shpFile);

        File prjFile = new File(tempDir, "grid.prj");
        filesByName.get(baseName + ".prj").write(prjFile);

        File dbfFile = new File(tempDir, "grid.dbf");
        filesByName.get(baseName + ".dbf").write(dbfFile);

        // shx is optional, not needed for dense shapefiles
        if (filesByName.containsKey(baseName + ".shx")) {
            File shxFile = new File(tempDir, "grid.shx");
            filesByName.get(baseName + ".shx").write(shxFile);
        }

        Map<String, Grid> grids = Grid.fromShapefile(shpFile, SeamlessCensusGridExtractor.ZOOM, (complete, total) -> {
            status.completedFeatures = complete;
            status.totalFeatures = total;
        });

        tempDir.delete();
        return grids;
    }

    private static List<Project.Indicator> writeGridsToS3 (Map<String, Grid> grids, String projectId, String dataSourceName) {
        // write all the grids to S3
        List<Project.Indicator> ret = new ArrayList<>();
        grids.forEach((field, grid) -> {
            String fieldKey = field.replaceAll(" ", "_").replaceAll("[^a-zA-Z0-9_\\-]+", "");
            String sourceKey = dataSourceName.replaceAll(" ", "_").replaceAll("[^a-zA-Z0-9_\\-]+", "");
            String key = String.format("%s_%s", fieldKey, sourceKey);
            String gridKey = String.format("%s/%s.grid", projectId, key);
            String pngKey = String.format("%s/%s.png", projectId, key);

            try {
                grid.write(GridExtractor.getOutputStream(AnalysisServerConfig.gridBucket, gridKey));
                grid.writePng(GridExtractor.getOutputStream(AnalysisServerConfig.gridBucket, pngKey));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Project.Indicator indicator = new Project.Indicator();
            indicator.key = key;
            indicator.name = field;
            indicator.dataSource = dataSourceName;
            ret.add(indicator);
        });

        return ret;
    }

    private static class GridUploadStatus {
        public int totalFeatures = -1;
        public int completedFeatures = -1;
        public String handle;
        public Status status;
    }

    private static enum Status {
        PROCESSING, ERROR, DONE;
    }

    public static void register () {
        get("/api/grid/status/:handle", GridController::getUploadStatus, JsonUtil.objectMapper::writeValueAsString);
        get("/api/grid/:projectId/:gridId", GridController::getGrid, JsonUtil.objectMapper::writeValueAsString);
        post("/api/grid/:projectId", GridController::createGrid, JsonUtil.objectMapper::writeValueAsString);
    }
}
