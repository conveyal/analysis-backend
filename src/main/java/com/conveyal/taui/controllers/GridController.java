package com.conveyal.taui.controllers;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.conveyal.taui.AnalystConfig;
import spark.Request;
import spark.Response;

import java.net.URL;
import java.util.Date;

import static spark.Spark.get;

/**
 * Controller that handles fetching grids.
 */
public class GridController {
    private static final AmazonS3 s3 = new AmazonS3Client();

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

    public static void register () {
        get("/api/grid/:projectId/:gridId", GridController::getGrid);
    }
}
