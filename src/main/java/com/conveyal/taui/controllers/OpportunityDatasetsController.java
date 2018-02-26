package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.grids.GridExporter;
import com.conveyal.taui.grids.SeamlessCensusGridExtractor;
import com.conveyal.taui.models.Region;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.Jobs;
import com.conveyal.taui.util.JsonUtil;
import com.google.common.io.Files;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.lang.Boolean.parseBoolean;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Controller that handles fetching grids.
 */
public class OpportunityDatasetsController {
    private static final Logger LOG = LoggerFactory.getLogger(OpportunityDatasetsController.class);

    private static final AmazonS3 s3 = new AmazonS3Client();
    private static final String BUCKET = AnalysisServerConfig.gridBucket;

    private static final FileItemFactory fileItemFactory = new DiskFileItemFactory();

    /**
     * Store upload status objects
     */
    private static List<OpportunityDatasetUploadStatus> uploadStatuses = new ArrayList<>();

    private static void addStatusAndRemoveOldStatuses(OpportunityDatasetUploadStatus status) {
        uploadStatuses.add(status);
        LocalDateTime now = LocalDateTime.now();
        uploadStatuses.removeIf(s -> s.completedAt != null &&
                LocalDateTime.ofInstant(s.completedAt.toInstant(), ZoneId.systemDefault()).isBefore(now.minusDays(7))
        );
    }

    public static Object getOpportunityDataset(Request req, Response res) {
        // TODO check region membership
        // TODO combine with downloadOpportunityDataset, which does not hardcode the "grid" format
        String key = String.format("%s/%s.grid", req.params("regionId"), req.params("gridId"));
        String redirectText = req.queryParams("redirect");
        boolean redirect = GridExporter.checkRedirectAndFormat(redirectText, "grid");

        // TODO handle offline mode
        return GridExporter.downloadFromS3(s3, BUCKET, key, redirect, res);
    }

    public static List<OpportunityDatasetUploadStatus> getRegionUploadStatuses(Request req, Response res) {
        String regionId = req.params("regionId");
        return uploadStatuses
                .stream()
                .filter(status -> {
                    Boolean b = status.regionId.equals(regionId);
                    return b;
                })
                .collect(Collectors.toList());
    }

    public static boolean clearStatus(Request req, Response res) {
        String statusId = req.params("statusId");
        return uploadStatuses.removeIf(s -> s.id.equals(statusId));
    }

    /**
     * Handle many types of file upload. Returns a OpportunityDatasetUploadStatus which has a handle to request status.
     */
    public static OpportunityDatasetUploadStatus createOpportunityDataset(Request req, Response res) {

        final String accessGroup = req.attribute("accessGroup");
        final String email = req.attribute("email");

        ServletFileUpload sfu = new ServletFileUpload(fileItemFactory);
        String dataSet;
        Map<String, List<FileItem>> query;
        try {
            query = sfu.parseParameterMap(req.raw());
            dataSet = query.get("Name").get(0).getString("UTF-8");
        } catch (Exception e) {
            throw AnalysisServerException.fileUpload("Unable to create opportunity dataset. " + e.getMessage());
        }

        String regionId = req.params("regionId");
        OpportunityDatasetUploadStatus status = new OpportunityDatasetUploadStatus(regionId, dataSet);

        addStatusAndRemoveOldStatuses(status);

        Jobs.service.submit(() -> {
            try {
                Map<String, Grid> grids = null;

                for (FileItem fi : query.get("files")) {
                    String name = fi.getName();
                    if (name.endsWith(".csv")) {
                        LOG.info("Detected opportunity dataset stored as CSV");
                        grids = createGridsFromCsv(query, status);
                        break;
                    } else if (name.endsWith(".grid")) {
                        LOG.info("Detected opportunity dataset stored in Conveyal binary format.");
                        grids = createGridsFromBinaryGridFiles(query, status);
                        break;
                    } else if (name.endsWith(".shp")) {
                        LOG.info("Detected opportunity dataset stored as shapefile");
                        grids = createGridsFromShapefile(query, fi.getName().substring(0, name.length() - 4), status);
                        break;
                    }
                }

                if (grids == null) {
                    status.status = Status.ERROR;
                    status.message = "Unable to create opportunity dataset from the files uploaded.";
                    status.completed();
                    return null;
                } else {
                    status.status = Status.UPLOADING;
                    status.totalGrids = grids.size();
                    LOG.info("Uploading opportunity dataset to S3");
                    List<Region.OpportunityDataset> opportunities = writeOpportunityDatasetToS3(grids, regionId, dataSet, status);
                    Region region = Persistence.regions.get(regionId).clone();
                    region.opportunityDatasets = new ArrayList<>(region.opportunityDatasets);
                    region.opportunityDatasets.addAll(opportunities);
                    Persistence.regions.updateByUserIfPermitted(region, email, accessGroup);
                    return opportunities;
                }
            } catch (HaltException e) {
                status.status = Status.ERROR;
                status.message = e.getBody();
                status.completed();
                return null;
            } catch (Exception e) {
                status.status = Status.ERROR;
                status.message = e.getMessage();
                status.completed();
                return null;
            }
        });

        return status;
    }

    public static Region.OpportunityDataset deleteOpportunityDataset(Request request, Response response) {
        String regionId = request.params("regionId");
        String gridId = request.params("gridId");
        Region region = Persistence.regions.get(regionId);
        boolean removed = region.opportunityDatasets.removeIf((od) -> od.key.equals(gridId));

        if (!removed) {
            throw AnalysisServerException.notFound("Opportunity dataset could not be found.");
        } else {
            Persistence.regions.updateByUserIfPermitted(region, request.attribute("email"), request.attribute("accessGroup"));
            s3.deleteObject(BUCKET, gridId);
        }

        return null;
    }

    /**
     * Create a grid from WGS 84 points in a CSV file
     */
    private static Map<String, Grid> createGridsFromCsv(Map<String, List<FileItem>> query, OpportunityDatasetUploadStatus status) throws Exception {
        String latField = query.get("latField").get(0).getString("UTF-8");
        String lonField = query.get("lonField").get(0).getString("UTF-8");

        List<FileItem> file = query.get("files");

        if (file.size() != 1) {
            throw AnalysisServerException.fileUpload("CSV upload only supports one file at a time.");
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
    private static Map<String, Grid> createGridsFromBinaryGridFiles(Map<String, List<FileItem>> query, OpportunityDatasetUploadStatus status) throws Exception {
        Map<String, Grid> grids = new HashMap<>();
        List<FileItem> uploadedFiles = query.get("files");
        status.totalFeatures = uploadedFiles.size();
        for (FileItem fileItem : uploadedFiles) {
            Grid grid = Grid.read(fileItem.getInputStream());
            grids.put(fileItem.getName(), grid);
        }
        status.completedFeatures = status.totalFeatures;
        return grids;
    }

    private static Map<String, Grid> createGridsFromShapefile(Map<String, List<FileItem>> query, String baseName, OpportunityDatasetUploadStatus status) throws Exception {
        // extract relevant files: .shp, .prj, .dbf, and .shx.
        // We need the SHX even though we're looping over every feature as they might be sparse.
        Map<String, FileItem> filesByName = query.get("files").stream()
                .collect(Collectors.toMap(FileItem::getName, f -> f));

        if (!filesByName.containsKey(baseName + ".shp") ||
                !filesByName.containsKey(baseName + ".prj") ||
                !filesByName.containsKey(baseName + ".dbf")) {
            throw AnalysisServerException.fileUpload("Shapefile upload must contain .shp, .prj, and .dbf");
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

    /**
     * Respond to a request with a redirect to a downloadable file.
     *
     * @req should specify regionId, gridId, and an available download format (.tiff or .grid)
     *
     */
    private static Object downloadOpportunityDataset (Request req, Response res) throws IOException {
        // TODO check region membership
        String regionId = req.params("regionId");
        String gridId = req.params("gridId");
        String gridPath = String.format("%s/%s", regionId, gridId);
        String format = req.params("format");
        String redirectText = req.queryParams("redirect");
        boolean redirect;
        redirect = redirectText == null || "".equals(redirectText) || parseBoolean(redirectText);

        if (!s3.doesObjectExist(BUCKET, String.format("%s.%s", gridPath, format))) {
            // if this grid is not on S3 in the requested format, try to get the .grid format
            if (!s3.doesObjectExist(BUCKET, String.format("%s.grid", gridPath))) {
                throw AnalysisServerException.notFound("This grid does not exist.");
            } else {
                // get the grid and convert it to the requested format
                S3Object s3Grid = s3.getObject(BUCKET, String.format("%s.grid", gridPath));
                InputStream rawInput = s3Grid.getObjectContent();
                Grid grid = Grid.read(new GZIPInputStream(rawInput));
                GridExporter.writeToS3(grid, s3, BUCKET, gridPath, format);
            }
        }
        return GridExporter.downloadFromS3(s3, BUCKET, String.format("%s.%s", gridPath, format), redirect, res);
    }

    private static List<Region.OpportunityDataset> writeOpportunityDatasetToS3(Map<String, Grid> grids, String regionId, String dataSourceName, OpportunityDatasetUploadStatus status) {
        // write all the grids to S3
        List<Region.OpportunityDataset> ret = new ArrayList<>();
        grids.forEach((field, grid) -> {
            String fieldKey = field.replaceAll(" ", "_").replaceAll("[^a-zA-Z0-9_\\-]+", "");
            String sourceKey = dataSourceName.replaceAll(" ", "_").replaceAll("[^a-zA-Z0-9_\\-]+", "");
            String key = String.format("%s_%s", fieldKey, sourceKey);
            String gridKey = String.format("%s/%s.grid", regionId, key);
            String pngKey = String.format("%s/%s.png", regionId, key);

            try {
                ByteArrayOutputStream gridByteStream = new ByteArrayOutputStream();
                grid.write(new GZIPOutputStream(gridByteStream)); // write to Stream -> LittleEndian -> Gzip -> Byte
                byte[] gridBytes = gridByteStream.toByteArray();
                ObjectMetadata gridMetadata = new ObjectMetadata();
                gridMetadata.setContentType("application/octet-stream");
                gridMetadata.setContentEncoding("gzip");
                gridMetadata.setContentLength(gridBytes.length);
                PutObjectRequest gridRequest = new PutObjectRequest(BUCKET, gridKey, new ByteArrayInputStream(gridBytes), gridMetadata);
                LOG.info("Uploading to {}", gridKey);
                s3.putObject(gridRequest);

                status.uploadedGrids += 1;
                if (status.uploadedGrids == status.totalGrids) {
                    status.status = Status.DONE;
                    status.completed();
                }
                LOG.info("Completed {}/{} uploads for {}", status.uploadedGrids, status.totalGrids, status.name);
            } catch (IOException e) {
                status.status = Status.ERROR;
                status.message = e.getMessage();
                status.completed();
                throw AnalysisServerException.unknown(e);
            }

            Region.OpportunityDataset opportunityDataset = new Region.OpportunityDataset();
            opportunityDataset.key = key;
            opportunityDataset.name = field;
            opportunityDataset.dataSource = dataSourceName;
            ret.add(opportunityDataset);
        });

        return ret;
    }

    private static class OpportunityDatasetUploadStatus {
        public String id;
        public int totalFeatures = 0;
        public int completedFeatures = 0;
        public int totalGrids = 0;
        public int uploadedGrids = 0;
        public String regionId;
        public Status status = Status.PROCESSING;
        public String name;
        public String message;
        public Date createdAt;
        public Date completedAt;

        public OpportunityDatasetUploadStatus(String regionId, String name) {
            this.id = UUID.randomUUID().toString();
            this.regionId = regionId;
            this.name = name;
            this.createdAt = new Date();
        }

        public void completed () {
            this.completedAt = new Date();
        }
    }

    private enum Status {
        UPLOADING, PROCESSING, ERROR, DONE;
    }

    public static void register() {
        delete("/api/opportunities/:regionId/:gridId", OpportunityDatasetsController::deleteOpportunityDataset, JsonUtil.objectMapper::writeValueAsString);
        delete("/api/opportunities/:regionId/status/:statusId", OpportunityDatasetsController::clearStatus, JsonUtil.objectMapper::writeValueAsString);
        get("/api/opportunities/:regionId/status", OpportunityDatasetsController::getRegionUploadStatuses, JsonUtil.objectMapper::writeValueAsString);
        get("/api/opportunities/:regionId/:gridId", OpportunityDatasetsController::getOpportunityDataset, JsonUtil.objectMapper::writeValueAsString);
        get("/api/opportunities/:regionId/:gridId/:format", OpportunityDatasetsController::downloadOpportunityDataset, JsonUtil.objectMapper::writeValueAsString);
        post("/api/opportunities/:regionId", OpportunityDatasetsController::createOpportunityDataset, JsonUtil.objectMapper::writeValueAsString);
    }
}
