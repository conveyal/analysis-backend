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

    public static List<Project.Indicator> createGrid (Request req, Response res) throws Exception {
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        Map<String, List<FileItem>> query = sfu.parseParameterMap(req.raw());
        String projectId = req.params("projectId");

        // TODO check project membership

        String dataSet = query.get("Name").get(0).getString("UTF-8");

        for (FileItem fi : query.get("files")) {
            if (fi.getName().endsWith(".csv")) {
                return createGridFromCsv(projectId, dataSet, query);
            }
        }

        halt(400);
        return null;
    }

    /** Create a grid from WGS 84 points in a CSV file */
    private static List<Project.Indicator> createGridFromCsv(String projectId, String dataSetName, Map<String, List<FileItem>> query) throws Exception {
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

        CsvReader reader = new CsvReader(new BufferedInputStream(new FileInputStream(tempFile)), Charset.forName("UTF-8"));
        reader.readHeaders();

        String[] headers = reader.getHeaders();
        if (!Stream.of(headers).filter(h -> h.equals(latField)).findAny().isPresent()) {
            LOG.info("Lat field not found!");
            halt(400, "Lat field not found");
        }

        if (!Stream.of(headers).filter(h -> h.equals(lonField)).findAny().isPresent()) {
            LOG.info("Lon field not found!");
            halt(400, "Lon field not found");
        }

        Envelope envelope = new Envelope();

        // Keep track of which fields contain numeric values
        Set<String> numericColumns = Stream.of(headers).collect(Collectors.toCollection(HashSet::new));
        numericColumns.remove(latField);
        numericColumns.remove(lonField);

        int i = 0;
        while (reader.readRecord()) {
            if (++i % 10000 == 0) LOG.info("{} records", human(i));

            envelope.expandToInclude(parseDouble(reader.get(lonField)), parseDouble(reader.get(latField)));

            for (String field : numericColumns) {
                String value = reader.get(field);
                if (value == null || "".equals(value)) continue; // allow missing data
                try {
                    // TODO also exclude columns containing negatives?
                    parseDouble(value);
                } catch (NumberFormatException e) {
                    numericColumns.remove(field);
                }
            }
        }

        reader.close();

        // We now have an envelope and know which columns are numeric
        // Make a grid for each numeric column
        Map<String, Grid> grids = numericColumns.stream()
                .collect(
                        Collectors.toMap(
                            c -> c,
                            c -> new Grid(
                                    SeamlessCensusGridFetcher.ZOOM,
                                    envelope.getMaxY(),
                                    envelope.getMaxX(),
                                    envelope.getMinY(),
                                    envelope.getMinX()
                )));

        // read it again, Sam - reread the CSV to get the actual values and populate the grids
        reader = new CsvReader(new BufferedInputStream(new FileInputStream(tempFile)), Charset.forName("UTF-8"));
        reader.readHeaders();

        i = 0;
        while (reader.readRecord()) {
            if (++i % 10000 == 0) LOG.info("{} records", human(i));

            double lat = parseDouble(reader.get(latField));
            double lon = parseDouble(reader.get(lonField));

            for (String field : numericColumns) {
                String value = reader.get(field);

                double val;

                if (value == null || "".equals(value)) {
                    val = 0;
                } else {
                    val = parseDouble(value);
                }

                grids.get(field).incrementPoint(lat, lon, val);
            }
        }

        // clean up
        reader.close();
        tempFile.delete();

        // write all the grids to S3
        List<Project.Indicator> ret = new ArrayList<>();
        for (String field : numericColumns) {
            Grid grid = grids.get(field);
            String fieldKey = field.replaceAll(" ", "_").replaceAll("[^a-zA-Z0-9_\\-]+", "");
            String sourceKey = dataSetName.replaceAll(" ", "_").replaceAll("[^a-zA-Z0-9_\\-]+", "");
            String key = String.format("%s_%s", fieldKey, sourceKey);
            String gridKey = String.format("%s/%s.grid", projectId, key);
            String pngKey = String.format("%s/%s.png", projectId, key);

            grid.write(GridFetcher.getOutputStream(AnalystConfig.gridBucket, gridKey));
            //grid.writePng(GridFetcher.getOutputStream(AnalystConfig.gridBucket, pngKey));

            Project.Indicator indicator = new Project.Indicator();
            indicator.key = key;
            indicator.name = field;
            indicator.dataSource = dataSetName;
            ret.add(indicator);
        }

        Project project = Persistence.projects.get(projectId).clone();
        project.indicators.addAll(ret);
        Persistence.projects.put(projectId, project);

        return ret;
    }

    public static void register () {
        get("/api/grid/:projectId/:gridId", GridController::getGrid);
        post("/api/grid/:projectId", GridController::createGrid);
    }
}
