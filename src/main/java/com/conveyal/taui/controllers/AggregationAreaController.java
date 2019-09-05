package com.conveyal.taui.controllers;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.util.S3Util;
import com.conveyal.r5.util.ShapefileReader;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.grids.SeamlessCensusGridExtractor;
import com.conveyal.taui.models.AggregationArea;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.union.UnaryUnionOp;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.json.simple.JSONObject;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Stores vector aggregationAreas (used to define the region of a weighted average accessibility metric).
 */
public class AggregationAreaController {
    private static final Logger LOG = LoggerFactory.getLogger(AggregationAreaController.class);
    private static final String awsRegion = AnalysisServerConfig.awsRegion;
    private static final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withRegion(awsRegion)
            .build();
    private static final FileItemFactory fileItemFactory = new DiskFileItemFactory();

    /**
     * Create binary .grid files for aggregation (aka mask) areas, save them to S3, and persist their details.
     * @param req Must include a shapefile on which the aggregation area(s) will be based.
     *
     *            Expected HTTP query parameters include "union" and "name." If "union==true", features will be merged
     *            to a single aggregation area, named using the value of the "name" query parameter directly.  If
     *            "union==false", each feature will be an aggregation area, named using its value for the shapefile
     *            property specified by "name."
     */

    public static List<AggregationArea> createAggregationAreas (Request req, Response res) throws Exception {
        ArrayList<AggregationArea> aggregationAreas = new ArrayList<>();
        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        Map<String, List<FileItem>> query = sfu.parseParameterMap(req.raw());

        // 1. Extract relevant files: .shp, .prj, .dbf, and .shx. ======================================================
        Map<String, FileItem> filesByName = query.get("files").stream()
                .collect(Collectors.toMap(FileItem::getName, f -> f));

        String fileName = filesByName.keySet().stream().filter(f -> f.endsWith(".shp")).findAny().orElse(null);
        if (fileName == null) {
            throw AnalysisServerException.fileUpload("Shapefile upload must contain .shp, .prj, and .dbf");
        }
        String baseName = fileName.substring(0, fileName.length() - 4);

        if (!filesByName.containsKey(baseName + ".shp") ||
                !filesByName.containsKey(baseName + ".prj") ||
                !filesByName.containsKey(baseName + ".dbf")) {
            throw AnalysisServerException.fileUpload("Shapefile upload must contain .shp, .prj, and .dbf");
        }

        String regionId = req.params("regionId");
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


        // 2. Read features ============================================================================================
        List<SimpleFeature> features = reader.wgs84Stream().collect(Collectors.toList());
        Map<String, Geometry> areas = new HashMap<>();

        if (Boolean.parseBoolean(req.params("union"))) {
            // Union (single combined aggregation area) requested
            List<Geometry> geometries = features.stream().map(f -> (Geometry) f.getDefaultGeometry()).collect(Collectors.toList());
            UnaryUnionOp union = new UnaryUnionOp(geometries);
            // Name the area using the name in the request directly
            areas.put(maskName, union.union());
        } else {
            // Don't union. Name each area by looking up its value for the name property in the request.
            features.stream().forEach(f -> areas.put(readProperty(f, maskName), (Geometry) f.getDefaultGeometry()));
        }
        // 3. Convert to raster grids, then store them. ================================================================
        areas.forEach((String name, Geometry geometry) -> {
            Envelope env = geometry.getEnvelopeInternal();
            Grid maskGrid = new Grid(SeamlessCensusGridExtractor.ZOOM, env.getMaxY(), env.getMaxX(), env.getMinY(), env.getMinX());

            // Store the percentage each cell overlaps the mask, scaled as 0 to 100,000
            List<Grid.PixelWeight> weights = maskGrid.getPixelWeights(geometry, true);
            weights.forEach(pixel -> maskGrid.grid[pixel.x][pixel.y] = pixel.weight * 100_000);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentEncoding("gzip");
            metadata.setContentType("application/octet-stream");

            AggregationArea aggregationArea = new AggregationArea();
            aggregationArea.name = name;
            aggregationArea.regionId = regionId;

            // Set `createdBy` and `accessGroup`
            aggregationArea.accessGroup = req.attribute("accessGroup");
            aggregationArea.createdBy = req.attribute("email");

            try {
                File gridFile = File.createTempFile(UUID.randomUUID().toString(),"grid");
                OutputStream os = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(gridFile)));
                maskGrid.write(os);
                os.close();

                // Create the aggregation area before generating the S3 key so that the `_id` is generated
                Persistence.aggregationAreas.create(aggregationArea);

                aggregationAreas.add(aggregationArea);

                InputStream is = new BufferedInputStream(new FileInputStream(gridFile));
                // can't use putObject with File when we have metadata . . .
                S3Util.s3.putObject(AnalysisServerConfig.gridBucket, aggregationArea.getS3Key(), is, metadata);
                is.close();
            } catch (IOException e) {
                throw new AnalysisServerException("Error processing/uploading aggregation area");
            }

            tempDir.delete();
        });

        return aggregationAreas;
    }

    static String readProperty (SimpleFeature feature, String propertyName) {
        try {
            return feature.getProperty(propertyName).getValue().toString();
        } catch (NullPointerException e) {
            throw new AnalysisServerException("The supplied Name was not a property of the uploaded features. " +
                    "Double check that the Name corresponds to a shapefile column.");
        }
    }

    public static Object getAggregationArea (Request req, Response res) {
        final String accessGroup = req.attribute("accessGroup");
        final String maskId = req.params("maskId");

        AggregationArea aggregationArea = Persistence.aggregationAreas.findByIdIfPermitted(maskId, accessGroup);

        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + 60 * 1000);
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(AnalysisServerConfig.gridBucket, aggregationArea.getS3Key(), HttpMethod.GET);
        request.setExpiration(expiration);

        URL url = s3.generatePresignedUrl(request);
        JSONObject wrappedUrl = new JSONObject();
        wrappedUrl.put("url", url.toString());

        return wrappedUrl;
    }

    public static void register () {
        get("/api/region/:regionId/aggregationArea/:maskId", AggregationAreaController::getAggregationArea, JsonUtil.objectMapper::writeValueAsString);
        post("/api/region/:regionId/aggregationArea", AggregationAreaController::createAggregationAreas,
                JsonUtil.objectMapper::writeValueAsString);
    }
}
