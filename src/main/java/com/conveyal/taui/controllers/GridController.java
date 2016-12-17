package com.conveyal.taui.controllers;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.grids.GridFetcher;
import com.conveyal.taui.grids.SeamlessCensusGridFetcher;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.persistence.Persistence;
import com.csvreader.CsvReader;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Envelope;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.conveyal.gtfs.util.Util.human;
import static java.lang.Double.parseDouble;
import static spark.Spark.get;
import static spark.Spark.halt;
import static spark.Spark.post;

/**
 * Controller that handles fetching grids.
 */
public class GridController {
    private static final Logger LOG = LoggerFactory.getLogger(GridController.class);

    private static final AmazonS3 s3 = new AmazonS3Client();

    private static final FileItemFactory fileItemFactory = new DiskFileItemFactory();

    /** How long request URLs are good for */
    public static final int REQUEST_TIMEOUT_MSEC = 15 * 1000;

    public static Object getGrid (Request req, Response res) {
        // TODO handle offline mode
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        // TODO check project membership

        String key = String.format("%s/%s.grid", req.params("projectId"), req.params("gridId"));

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(AnalystConfig.gridBucket, key);
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);

        URL url = s3.generatePresignedUrl(presigned);

        res.redirect(url.toString());
        res.status(302); // temporary redirect, this URL will soon expire
        return res;
    }

    /** Handle many types of file upload */
    // TODO process async
    public static List<Project.Indicator> createGrid (Request req, Response res) throws Exception {
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        Map<String, List<FileItem>> query = sfu.parseParameterMap(req.raw());
        String projectId = req.params("projectId");

        // TODO check project membership

        String dataSet = query.get("Name").get(0).getString("UTF-8");

        Map<String, Grid> grids = null;

        for (FileItem fi : query.get("files")) {
            String name = fi.getName();
            if (name.endsWith(".csv")) {
                LOG.info("Detected grid stored as CSV");
                grids = createGridsFromCsv(query);
                break;
            } else if (name.endsWith(".shp")) {
                LOG.info("Detected grid stored as shapefile");
                grids = createGridsFromShapefile(query, fi.getName().substring(0, name.length() - 4));
                break;
            }
        }

        if (grids == null) {
            halt(400);
            return null;
        } else {
            List<Project.Indicator> indicators = writeGridsToS3(grids, projectId, dataSet);
            Project project = Persistence.projects.get(projectId).clone();
            project.indicators = new ArrayList<>(project.indicators);
            project.indicators.addAll(indicators);
            Persistence.projects.put(projectId, project);
            return indicators;
        }
    }

    /** Create a grid from WGS 84 points in a CSV file */
    private static Map<String, Grid> createGridsFromCsv(Map<String, List<FileItem>> query) throws Exception {
        String latField = query.get("latField").get(0).getString("UTF-8");
        String lonField = query.get("lonField").get(0).getString("UTF-8");

        List<FileItem> file = query.get("files");

        if (file.size() != 1) {
            LOG.warn("CSV upload only supports one file at a time");
            halt(400);
        }

        // create a temp file because we have to loop over it twice
        File tempFile = File.createTempFile("grid", ".csv");
        file.get(0).write(tempFile);

        Map<String, Grid> grids = Grid.fromCsv(tempFile, latField, lonField, SeamlessCensusGridFetcher.ZOOM);
        // clean up
        tempFile.delete();

        return grids;
    }

    private static Map<String, Grid> createGridsFromShapefile (Map<String, List<FileItem>> query, String baseName) throws Exception {
        // extract relevant files: .shp, .prj, .dbf, and .shx.
        // We need the SHX even though we're looping over every feature as they might be sparse.
        Map<String, FileItem> filesByName = query.get("files").stream()
                .collect(Collectors.toMap(FileItem::getName, f -> f));

        if (!filesByName.containsKey(baseName + ".shp") ||
                    !filesByName.containsKey(baseName + ".prj") ||
                    !filesByName.containsKey(baseName + ".dbf")) {
            halt(400, "Shapefile upload must contain .shp, .prj, and .dbf");
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

        Map<String, Grid> grids = Grid.fromShapefile(shpFile, SeamlessCensusGridFetcher.ZOOM);
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
                grid.write(GridFetcher.getOutputStream(AnalystConfig.gridBucket, gridKey));
                grid.writePng(GridFetcher.getOutputStream(AnalystConfig.gridBucket, pngKey));
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

    public static void register () {
        get("/api/grid/:projectId/:gridId", GridController::getGrid);
        post("/api/grid/:projectId", GridController::createGrid);
    }
}
