package com.conveyal.analysis.controllers;

import com.conveyal.analysis.AnalysisServerException;
import com.conveyal.analysis.SelectingGridReducer;
import com.conveyal.analysis.components.broker.Broker;
import com.conveyal.analysis.components.broker.JobStatus;
import com.conveyal.analysis.components.broker.WorkerTags;
import com.conveyal.analysis.models.AnalysisRequest;
import com.conveyal.analysis.models.Project;
import com.conveyal.analysis.models.RegionalAnalysis;
import com.conveyal.analysis.persistence.Persistence;
import com.conveyal.analysis.util.JsonUtil;
import com.conveyal.file.FileStorage;
import com.conveyal.file.FileStorageFormat;
import com.conveyal.file.FileStorageKey;
import com.conveyal.file.FileUtils;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.scenario.Scenario;
import com.google.common.primitives.Ints;
import com.mongodb.QueryBuilder;
import gnu.trove.list.array.TIntArrayList;
import org.json.simple.JSONObject;
import org.mongojack.DBProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static com.conveyal.analysis.util.JsonUtil.toJson;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Spark HTTP handler methods that allow launching new regional analyses, as well as deleting them and fetching
 * information about them.
 */
public class RegionalAnalysisController implements HttpController {

    /** Until regional analysis config supplies percentiles in the request, hard-wire to our standard five. */
    private static final int[] DEFAULT_REGIONAL_PERCENTILES = new int[] {5, 25, 50, 75, 95};

    /**
     * Until the UI supplies cutoffs in the AnalysisRequest, hard-wire cutoffs.
     * The highest one is half our absolute upper limit of 120 minutes, which should by default save compute time.
     */
    public static final int[] DEFAULT_CUTOFFS = new int[] {5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60};

    private static final Logger LOG = LoggerFactory.getLogger(RegionalAnalysisController.class);

    private final Broker broker;
    private final FileStorage fileStorage;
    private final Config config;

    public interface Config {
        String resultsBucket ();
        String bundleBucket ();
    }

    public RegionalAnalysisController (Broker broker, FileStorage fileStorage, Config config) {
        this.broker = broker;
        this.fileStorage = fileStorage;
        this.config = config;
    }

    private Collection<RegionalAnalysis> getRegionalAnalysesForRegion(String regionId, String accessGroup) {
        return Persistence.regionalAnalyses.findPermitted(
                QueryBuilder.start().and(
                        QueryBuilder.start("regionId").is(regionId).get(),
                        QueryBuilder.start("deleted").is(false).get()
                ).get(),
                DBProjection.exclude("request.scenario.modifications"),
                accessGroup
        );
    }

    private Collection<RegionalAnalysis> getRegionalAnalysesForRegion(Request req, Response res) {
        return getRegionalAnalysesForRegion(req.params("regionId"), req.attribute("accessGroup"));
    }

    /**
     * Looks up all regional analyses for a region and checks the broker for jobs associated with them. If a JobStatus
     * exists it adds it to the list of running analyses.
     * @return JobStatues with associated regional analysis embedded
     */
    private Collection<JobStatus> getRunningAnalyses(Request req, Response res) {
        Collection<RegionalAnalysis> allAnalysesInRegion = getRegionalAnalysesForRegion(req.params("regionId"), req.attribute("accessGroup"));
        List<JobStatus> runningStatusesForRegion = new ArrayList<>();
        Collection<JobStatus> allJobStatuses = broker.getAllJobStatuses();
        for (RegionalAnalysis ra : allAnalysesInRegion) {
            JobStatus jobStatus = allJobStatuses.stream().filter(j -> j.jobId.equals(ra._id)).findFirst().orElse(null);
            if (jobStatus != null) {
                jobStatus.regionalAnalysis = ra;
                runningStatusesForRegion.add(jobStatus);
            }
        }

        return runningStatusesForRegion;
    }

    private RegionalAnalysis deleteRegionalAnalysis (Request req, Response res) {
        String accessGroup = req.attribute("accessGroup");
        String email = req.attribute("email");

        RegionalAnalysis analysis = Persistence.regionalAnalyses.findPermitted(
                QueryBuilder.start().and(
                        QueryBuilder.start("_id").is(req.params("_id")).get(),
                        QueryBuilder.start("deleted").is(false).get()
                ).get(),
                DBProjection.exclude("request.scenario.modifications"),
                accessGroup
        ).iterator().next();
        analysis.deleted = true;
        Persistence.regionalAnalyses.updateByUserIfPermitted(analysis, email, accessGroup);

        // clear it from the broker
        if (!analysis.complete) {
            String jobId = analysis._id;
            if (broker.deleteJob(jobId)) {
                LOG.info("Deleted job {} from broker.", jobId);
            } else {
                LOG.error("Deleting job {} from broker failed.", jobId);
            }
        }
        return analysis;
    }

    private int getIntQueryParameter (Request req, String parameterName, int defaultValue) {
        String paramValue = req.queryParams(parameterName);
        if (paramValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(paramValue);
        } catch (Exception ex) {
            String message = String.format(
                "Query parameter '%s' must be an integer, cannot parse '%s'.",
                parameterName,
                paramValue
            );
            throw new IllegalArgumentException(message, ex);
        }
    }

    /**
     * This used to extract a particular percentile of a regional analysis as a grid file.
     * Now it just gets the single percentile that exists for any one analysis, either from the local buffer file
     * for an analysis still in progress, or from S3 for a completed analysis.
     */
    private Object getRegionalResults (Request req, Response res) throws IOException {

        // Get some path parameters out of the URL.
        // The UUID of the regional analysis for which we want the output data
        final String regionalAnalysisId = req.params("_id");
        // The response file format: PNG, TIFF, or GRID
        final String fileFormatExtension = req.params("format");

        RegionalAnalysis analysis = Persistence.regionalAnalyses.findPermitted(
                QueryBuilder.start("_id").is(req.params("_id")).get(),
                DBProjection.exclude("request.scenario.modifications"),
                req.attribute("accessGroup")
        ).iterator().next();

        if (analysis == null || analysis.deleted) {
            throw AnalysisServerException.notFound("The specified regional analysis in unknown or has been deleted.");
        }

        // Which channel to extract from results with multiple values per origin (for different travel time cutoffs)
        // and multiple output files per analysis (for different percentiles of travel time). The base case is older
        // regional analyses with only a single cutoff, and no percentile in the file name.
        int percentile = analysis.travelTimePercentile;
        int cutoffMinutes = analysis.cutoffMinutes;
        int cutoffIndex = 0;

        // Handle newer regional analyses with multiple cutoffs in an array. If a query parameter is supplied, range
        // check it, otherwise use the middle value in the list.
        if (analysis.cutoffsMinutes != null) {
            int nCutoffs = analysis.cutoffsMinutes.length;
            checkState(nCutoffs > 0, "Regional analysis has no cutoffs.");
            cutoffMinutes = getIntQueryParameter(req, "cutoff", analysis.cutoffsMinutes[nCutoffs / 2]);
            cutoffIndex = new TIntArrayList(analysis.cutoffsMinutes).indexOf(cutoffMinutes);
            checkArgument(cutoffIndex >= 0,
                    "Travel time cutoff for this regional analysis must be taken from this list: (%s)",
                    Ints.join(", ", analysis.cutoffsMinutes)
            );
        }

        // Handle newer regional analyses with multiple percentiles in an array.
        // The percentile variable holds the actual percentile (25, 50, 95) not the position in the array.
        if (analysis.travelTimePercentiles != null) {
            int nPercentiles = analysis.travelTimePercentiles.length;
            checkState(nPercentiles > 0, "Regional analysis has no percentiles.");
            percentile = getIntQueryParameter(req, "percentile", analysis.travelTimePercentiles[nPercentiles / 2]);
            checkArgument(new TIntArrayList(analysis.travelTimePercentiles).contains(percentile),
                    "Percentile for this regional analysis must be taken from this list: (%s)",
                    Ints.join(", ", analysis.travelTimePercentiles));
        }

        // It seems like you would check regionalAnalysis.complete to choose between redirecting to s3 and fetching
        // the partially completed local file. But this field is never set to true - it's on a UI model object that
        // isn't readily accessible to the internal Job-tracking mechanism of the back end. Instead, just try to fetch
        // the partially completed results file, which includes an O(1) check whether the job is still being processed.
        File partialRegionalAnalysisResultFile = broker.getPartialRegionalAnalysisResults(regionalAnalysisId);

        if (partialRegionalAnalysisResultFile != null) {
            // FIXME we need to do the equivalent of the SelectingGridReducer here.
            // The job is still being processed. There is a probably harmless race condition if the job happens to be
            // completed at the very moment we're in this block, because the file will be deleted at that moment.
            LOG.info("Analysis {} is not complete, attempting to return the partial results grid.", regionalAnalysisId);
            if (!"GRID".equalsIgnoreCase(fileFormatExtension)) {
                throw AnalysisServerException.badRequest(
                        "For partially completed regional analyses, we can only return grid files, not images.");
            }
            if (partialRegionalAnalysisResultFile == null) {
                throw AnalysisServerException.unknown(
                        "Could not find partial result grid for incomplete regional analysis on server.");
            }
            try {
                res.header("content-type", "application/octet-stream");
                // This will cause Spark Framework to gzip the data automatically if requested by the client.
                res.header("Content-Encoding", "gzip");
                // Spark has default serializers for InputStream and Bytes, and calls toString() on everything else.
                return new FileInputStream(partialRegionalAnalysisResultFile);
            } catch (FileNotFoundException e) {
                // The job must have finished and the file was deleted upon upload to S3. This should be very rare.
                throw AnalysisServerException.unknown(
                        "Could not find partial result grid for incomplete regional analysis on server.");
            }
        } else {
            // The analysis has already completed, results should be stored and retrieved from S3 via redirects.
            LOG.info("Returning {} minute accessibility (percentile {}) for regional analysis {}.",
                    cutoffMinutes, percentile, regionalAnalysisId);
            FileStorageFormat format = FileStorageFormat.valueOf(fileFormatExtension.toUpperCase());
            if (!FileStorageFormat.GRID.equals(format) && !FileStorageFormat.PNG.equals(format) && !FileStorageFormat.TIFF.equals(format)) {
                throw AnalysisServerException.badRequest("Format \"" + format + "\" is invalid. Request format must be \"grid\", \"png\", or \"tiff\".");
            }

            // Analysis grids now have the percentile and cutoff in their S3 key, because there can be many of each.
            // We do this even for results generated by older workers, so they will be re-extracted with the new name.
            // These grids are reasonably small, we may be able to just send all cutoffs to the UI instead of selecting.
            String singleCutoffKey =
                    String.format("%s_P%d_C%d.%s", regionalAnalysisId, percentile, cutoffMinutes, fileFormatExtension);

            // A lot of overhead here - UI contacts backend, backend calls S3, backend responds to UI, UI contacts S3.
            FileStorageKey singleCutoffFileStorageKey = new FileStorageKey(config.resultsBucket(), singleCutoffKey);
            if (!fileStorage.exists(singleCutoffFileStorageKey)) {
                // Old single-percentile regional analysis result grids don't have a percentile component in the S3 key.
                String multiCutoffKey = (analysis.travelTimePercentiles == null)
                        ? regionalAnalysisId + ".access"
                        : String.format("%s_P%d.access", regionalAnalysisId, percentile);
                LOG.info("Single-cutoff grid {} not found on S3, deriving it from {}.", singleCutoffKey, multiCutoffKey);
                FileStorageKey multiCutoffFileStorageKey = new FileStorageKey(config.resultsBucket(), multiCutoffKey);

                InputStream multiCutoffInputStream = new FileInputStream(fileStorage.getFile(multiCutoffFileStorageKey));
                Grid grid = new SelectingGridReducer(cutoffIndex).compute(multiCutoffInputStream);

                File localFile = FileUtils.createScratchFile(format.toString());
                FileOutputStream fos = new FileOutputStream(localFile);

                switch (format) {
                    case GRID:
                        grid.write(new GZIPOutputStream(fos));
                        break;
                    case PNG:
                        grid.writePng(fos);
                        break;
                    case TIFF:
                        grid.writeGeotiff(fos);
                        break;
                }

                fileStorage.moveIntoStorage(singleCutoffFileStorageKey, localFile);
            }

            JSONObject json = new JSONObject();
            json.put("url", fileStorage.getURL(singleCutoffFileStorageKey));
            return json.toJSONString();
        }
    }

    /**
     * Deserialize a description of a new regional analysis (an AnalysisRequest object) POSTed as JSON over the HTTP API.
     * Derive an internal RegionalAnalysis object, which is enqueued in the broker and also returned to the caller
     * in the body of the HTTP response.
     */
    private RegionalAnalysis createRegionalAnalysis (Request req, Response res) throws IOException {
        final String accessGroup = req.attribute("accessGroup");
        final String email = req.attribute("email");

        AnalysisRequest analysisRequest = JsonUtil.objectMapper.readValue(req.body(), AnalysisRequest.class);

        // If the UI has requested creation of a "static site", set all the necessary switches on the requests
        // that will go to the worker: break travel time down into waiting, riding, and walking, record paths to
        // destinations, and save results on S3.
        if (analysisRequest.name.contains("STATIC_SITE")) {
            // Hidden feature: allows us to run static sites experimentally without exposing a checkbox to all users.
            analysisRequest.makeTauiSite = true;
        }

        if (analysisRequest.name.contains("MULTI_CUTOFF")) {
            // Hidden feature: allows us to test multiple cutoffs and percentiles without modifying UI.
            // These arrays could also be sent in the API payload. Either way, they will override any single cutoff.
            analysisRequest.cutoffsMinutes = DEFAULT_CUTOFFS;
            analysisRequest.percentiles = DEFAULT_REGIONAL_PERCENTILES;
        }

        // Create an internal RegionalTask and RegionalAnalysis from the AnalysisRequest sent by the client.
        Project project = Persistence.projects.findByIdIfPermitted(analysisRequest.projectId, accessGroup);
        // TODO now this is setting cutoffs and percentiles in the regional (template) task.
        //   why is some stuff set in this populate method, and other things set here in the caller?
        RegionalTask task = (RegionalTask) analysisRequest.populateTask(new RegionalTask(), project);

        // Set the destination pointset (which is still called "grid" for backward worker compatibility).
        task.grid = Persistence.opportunityDatasets
                .findByIdIfPermitted(analysisRequest.opportunityDatasetId, accessGroup).storageLocation();

        // Set the origin pointset if one is specified.
        if (analysisRequest.originPointSetId != null) {
            task.originPointSetKey = Persistence.opportunityDatasets
                    .findByIdIfPermitted(analysisRequest.originPointSetId, accessGroup).storageLocation();
        }

        task.oneToOne = analysisRequest.oneToOne;
        task.recordTimes = analysisRequest.recordTimes;
        task.recordAccessibility = analysisRequest.recordAccessibility;

        // Making a static site implies several different processes - turn them all on if requested.
        if (analysisRequest.makeTauiSite) {
            task.makeTauiSite = true;
            task.computeTravelTimeBreakdown = true;
            task.computePaths = true;
            task.recordAccessibility = false;
        }

        // TODO remove duplicate fields from RegionalAnalysis that are already in the nested task.
        // The RegionalAnalysis should just be a minimal wrapper around the template task, adding the origin point set.
        // The RegionalAnalysis object contains a reference to the template task itself.
        // In fact, there are three separate classes all containing almost the same info:
        // AnalysisRequest, RegionalTask, RegionalAnalysis.
        RegionalAnalysis regionalAnalysis = new RegionalAnalysis();
        regionalAnalysis.request = task;
        regionalAnalysis.height = task.height;
        regionalAnalysis.north = task.north;
        regionalAnalysis.west = task.west;
        regionalAnalysis.width = task.width;

        regionalAnalysis.accessGroup = accessGroup;
        regionalAnalysis.bundleId = project.bundleId;
        regionalAnalysis.createdBy = email;
        regionalAnalysis.grid = analysisRequest.opportunityDatasetId;
        regionalAnalysis.name = analysisRequest.name;
        regionalAnalysis.projectId = analysisRequest.projectId;
        regionalAnalysis.regionId = project.regionId;
        regionalAnalysis.variant = analysisRequest.variantIndex;
        regionalAnalysis.workerVersion = analysisRequest.workerVersion;
        regionalAnalysis.zoom = task.zoom;

        // Handle new multiple cutoff and multiple percentile arrays if they are present.
        if (analysisRequest.cutoffsMinutes != null) {
            // Store invalid value (-1 was already used) in deprecated field to make it clear it should not be used.
            regionalAnalysis.cutoffMinutes = -2;
            // Store the full array which will be read by newer workers and backends.
            regionalAnalysis.cutoffsMinutes = analysisRequest.cutoffsMinutes;
        } else {
            // Replicate the older system, with a single cutoff. This ensures older workers will still function.
            regionalAnalysis.cutoffMinutes = task.maxTripDurationMinutes;
            // Create a one-element array so newer workers won't have to check deprecated non-array fields.
            regionalAnalysis.cutoffsMinutes = new int[] { regionalAnalysis.cutoffMinutes };
        }
        // Same process as for cutoffsMinutes, but for percentiles UI has been observed to send one-element arrays.
        if (analysisRequest.percentiles != null) {
            if (analysisRequest.percentiles.length == 1) {
                regionalAnalysis.travelTimePercentile = analysisRequest.percentiles[0];
            } else {
                regionalAnalysis.travelTimePercentile = -2;
            }
            regionalAnalysis.travelTimePercentiles = analysisRequest.percentiles;
        } else {
            regionalAnalysis.travelTimePercentile = analysisRequest.travelTimePercentile;
            regionalAnalysis.travelTimePercentiles = new int[] { regionalAnalysis.travelTimePercentile };
        }

        // Propagate any changes to the cutoff and percentiles arrays down into the nested RegionalTask.
        task.cutoffsMinutes = regionalAnalysis.cutoffsMinutes;
        task.percentiles = regionalAnalysis.travelTimePercentiles;

        // Set the maximum trip duration just high enough to compute accessibility for the highest cutoff.
        task.maxTripDurationMinutes = Arrays.stream(regionalAnalysis.cutoffsMinutes).max().getAsInt();

        // Persist this newly created RegionalAnalysis to Mongo.
        // Why are we overwriting the regionalAnalysis reference with the result of saving it? This looks like a no-op.
        regionalAnalysis = Persistence.regionalAnalyses.create(regionalAnalysis);

        // The single RegionalAnalysis object represents a lot of individual accessibility tasks at many different
        // origin points, typically on a grid. Before passing that object on to the Broker (which distributes tasks to
        // workers and tracks progress), we remove the details of the scenario, substituting the scenario's unique ID
        // to save time and bandwidth. This avoids repeatedly sending the scenario details to the worker in every task,
        // as they are often quite voluminous. The workers will fetch the scenario once from S3 and cache it based on
        // its ID only. We protectively clone this task because we're going to null out its scenario field, and don't
        // want to affect the original object which contains all the scenario details.
        // TODO why is the request object cloned? Why is all this detail added after the Persistence call?
        //      We don't want to store all the details added below in Mongo?
        RegionalTask templateTask = regionalAnalysis.request.clone();
        Scenario scenario = templateTask.scenario;
        templateTask.scenarioId = scenario.id;
        templateTask.scenario = null;
        String fileName = String.format("%s_%s.json", regionalAnalysis.bundleId, scenario.id);
        FileStorageKey fileStorageKey = new FileStorageKey(config.bundleBucket(), fileName);
        try {
            File localScenario = FileUtils.createScratchFile("json");
            JsonUtil.objectMapper.writeValue(localScenario, scenario);
            fileStorage.moveIntoStorage(fileStorageKey, localScenario);
        } catch (IOException e) {
            LOG.error("Error saving scenario to disk", e);
        }

        // Fill in all the fields in the template task that will remain the same across all tasks in a job.
        // Re-setting all these fields may not be necessary (they might already be set previously),
        // but we can't eliminate these lines without thoroughly checking that assumption.
        templateTask.jobId = regionalAnalysis._id;
        templateTask.graphId = regionalAnalysis.bundleId;
        templateTask.workerVersion = regionalAnalysis.workerVersion;
        templateTask.height = regionalAnalysis.height;
        templateTask.width = regionalAnalysis.width;
        templateTask.north = regionalAnalysis.north;
        templateTask.west = regionalAnalysis.west;
        templateTask.zoom = regionalAnalysis.zoom;

        // Register the regional job with the broker, which will distribute individual tasks to workers and track progress.
        broker.enqueueTasksForRegionalJob(templateTask, WorkerTags.fromRegionalAnalysis(regionalAnalysis));

        return regionalAnalysis;
    }

    private RegionalAnalysis updateRegionalAnalysis(Request request, Response response) throws IOException {
        final String accessGroup = request.attribute("accessGroup");
        final String email = request.attribute("email");
        RegionalAnalysis regionalAnalysis = JsonUtil.objectMapper.readValue(request.body(), RegionalAnalysis.class);
        return Persistence.regionalAnalyses.updateByUserIfPermitted(regionalAnalysis, email, accessGroup);
    }

    @Override
    public void registerEndpoints (spark.Service sparkService) {
        sparkService.path("/api/region", () -> {
            sparkService.get("/:regionId/regional", this::getRegionalAnalysesForRegion, toJson);
            sparkService.get("/:regionId/regional/running", this::getRunningAnalyses, toJson);
        });
        sparkService.path("/api/regional", () -> {
            // For grids, no transformer is supplied: render raw bytes or input stream rather than transforming to JSON.
            sparkService.get("/:_id/grid/:format", this::getRegionalResults);
            sparkService.delete("/:_id", this::deleteRegionalAnalysis, toJson);
            sparkService.post("", this::createRegionalAnalysis, toJson);
            sparkService.put("/:_id", this::updateRegionalAnalysis, toJson);
        });
    }

}
