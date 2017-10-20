package com.conveyal.taui.controllers;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.Grid.PixelWeight;
import com.conveyal.r5.util.S3Util;
import com.conveyal.r5.util.ShapefileReader;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.grids.SeamlessCensusGridExtractor;
import com.conveyal.taui.models.AggregationArea;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.conveyal.taui.util.WrappedURL;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.union.UnaryUnionOp;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static com.conveyal.taui.util.SparkUtil.haltWithJson;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Stores vector aggregationAreas (used to define the region of a weighted average accessibility metric).
 */
public class AggregationAreaController {
    private static final Logger LOG = LoggerFactory.getLogger(AggregationAreaController.class);

    private static final AmazonS3 s3 = new AmazonS3Client();

    private static final FileItemFactory fileItemFactory = new DiskFileItemFactory();

    public static AggregationArea createAggregationArea (Request req, Response res) throws Exception {
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        Map<String, List<FileItem>> query = sfu.parseParameterMap(req.raw());

        // extract relevant files: .shp, .prj, .dbf, and .shx.
        // We need the SHX even though we're looping over every feature as they might be sparse.
        Map<String, FileItem> filesByName = query.get("files").stream()
                .collect(Collectors.toMap(FileItem::getName, f -> f));

        String fileName = filesByName.keySet().stream().filter(f -> f.endsWith(".shp")).findAny().orElse(null);
        if (fileName == null) {
            haltWithJson(400, "Shapefile upload must contain .shp, .prj, and .dbf. Please verify the contents of your data before trying again.");
        }
        String baseName = fileName.substring(0, fileName.length() - 4);

        if (!filesByName.containsKey(baseName + ".shp") ||
                !filesByName.containsKey(baseName + ".prj") ||
                !filesByName.containsKey(baseName + ".dbf")) {
            haltWithJson(400, "Shapefile upload must contain .shp, .prj, and .dbf. Please verify the contents of your data before trying again.");
        }

        String projectId = req.params("projectId");
        String maskName = query.get("name").get(0).getString("UTF-8");

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

        ShapefileReader reader = new ShapefileReader(shpFile);

        List<Geometry> geometries = reader.stream().map(f -> (Geometry) f.getDefaultGeometry()).collect(Collectors.toList());
        UnaryUnionOp union = new UnaryUnionOp(geometries);
        Geometry merged = union.union();

        Envelope env = merged.getEnvelopeInternal();
        Grid maskGrid = new Grid(SeamlessCensusGridExtractor.ZOOM, env.getMaxY(), env.getMaxX(), env.getMinY(), env.getMinX());

        // Store the percentage each cell overlaps the mask, scaled as 0 to 100,000
        ArrayList<PixelWeight> weights = maskGrid.getPixelWeights(merged, true);
        weights.forEach((pixel) -> {
            maskGrid.grid[pixel.getX()][pixel.getY()] = pixel.getWeight() * 100_000;
        });

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentEncoding("gzip");
        metadata.setContentType("application/octet-stream");

        AggregationArea aggregationArea = new AggregationArea();
        aggregationArea.name = maskName;
        aggregationArea.id = UUID.randomUUID().toString();
        aggregationArea.projectId = projectId;

        File gridFile = new File(tempDir, "weights.grid");
        OutputStream os = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(gridFile)));
        maskGrid.write(os);
        os.close();

        InputStream is = new BufferedInputStream(new FileInputStream(gridFile));
        // can't use putObject with File when we have metadata . . .
        S3Util.s3.putObject(AnalysisServerConfig.gridBucket, aggregationArea.getS3Key(), is, metadata);
        is.close();

        Persistence.aggregationAreas.put(aggregationArea.id, aggregationArea);

        tempDir.delete();

        return aggregationArea;
    }

    public static Object getAggregationArea (Request req, Response res) {
        String maskId = req.params("maskId");
        String projectId = req.params("projectId");
        boolean redirect = true;

        try {
            String redirectParam = req.queryParams("redirect");
            if (redirectParam != null) redirect = Boolean.parseBoolean(redirectParam);
        } catch (Exception e) {
            // do nothing
        }

        AggregationArea aggregationArea = Persistence.aggregationAreas.get(maskId);

        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + 60 * 1000);
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(AnalysisServerConfig.gridBucket, aggregationArea.getS3Key(), HttpMethod.GET);
        request.setExpiration(expiration);

        URL url = s3.generatePresignedUrl(request);

        if (redirect) {
            res.redirect(url.toString());
            res.status(302); // temporary redirect, this URL will soon expire
            res.type("text/plain"); // override application/json default
            return res;
        } else {
            return new WrappedURL(url);
        }
    }

    public static void register () {
        get("/api/project/:projectId/aggregationArea/:maskId", AggregationAreaController::getAggregationArea, JsonUtil.objectMapper::writeValueAsString);
        post("/api/project/:projectId/aggregationArea", AggregationAreaController::createAggregationArea, JsonUtil.objectMapper::writeValueAsString);
    }
}
