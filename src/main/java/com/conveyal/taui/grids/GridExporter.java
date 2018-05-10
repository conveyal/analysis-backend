package com.conveyal.taui.grids;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.util.WrappedURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Response;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URL;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

import static java.lang.Boolean.parseBoolean;

/**
 * Class with methods to write grids to specified formats, upload to S3, and direct clients to download them.
 *
 * Created by ansoncfit on 27-Nov-17, adapting code created by matthewc on 21-Oct-16
 */
public abstract class GridExporter {

    private static final Logger LOG = LoggerFactory.getLogger(GridExporter.class);

    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    /** How long request URLs are good for */
    public static final int REQUEST_TIMEOUT_MSEC = 15 * 1000;

    private static void haltWithIncorrectFormat (String format) {
        throw AnalysisServerException.badRequest("Format \"" + format + "\" is invalid. Request format must be \"grid\", \"png\", or \"tiff\".");
    }

    /**
     * Checks two request parameters, ensuring that the requested download format is one of the allowable ones,
     * and returning a parsed boolean of whether a redirect was requested
     *
     * @param redirectText whether to redirect the response
     * @param format requested format
     * @return cleaned up version of redirectText
     */
    public static boolean checkRedirectAndFormat(String redirectText, String format){
        // FIXME replace string matching with enum type
        if (!"grid".equals(format) && !"png".equals(format) && !"tiff".equals(format)) {
            haltWithIncorrectFormat(format);
        }
        boolean redirect;
        if (redirectText == null || "" .equals(redirectText)) {
            redirect = true;
        } else {
            redirect = parseBoolean(redirectText);
        }
        return redirect;
    }

    /**
     * Writes a grid to S3 with requested format
     *
     * @param grid grid to write
     * @param s3 s3 instance
     * @param bucket name of the bucket
     * @param key
     * @param format allowable formats include grid (Conveyal flat binary format), png, and tiff.
     * @throws IOException
     */
    public static void writeToS3(Grid grid, AmazonS3 s3, String bucket, String key, String format) throws IOException {
        PipedInputStream pis = new PipedInputStream();
        PipedOutputStream pos = new PipedOutputStream(pis);

        ObjectMetadata om = new ObjectMetadata();
        // FIXME replace string matching with enum type
        if ("grid".equals(format)) {
            om.setContentType("application/octet-stream");
            om.setContentEncoding("gzip");
        } else if ("png".equals(format)) {
            om.setContentType("image/png");
        } else if ("tiff".equals(format)) {
            om.setContentType("image/tiff");
        }
        
        // We run the write task in an executor that runs it in a separate thread mostly because the S3 library requires
        // an inputstream in putObject, and the geotiff writer library requires an outputstream, so we need to pipe
        // between threads.
        executorService.execute(() -> {
            try {
                if ("grid".equals(format)) {
                    grid.write(new GZIPOutputStream(pos));
                } else if ("png".equals(format)) {
                    grid.writePng(pos);
                } else if ("tiff".equals(format)) {
                    grid.writeGeotiff(pos);
                }
            } catch (IOException e) {
                LOG.info("Error writing percentile to S3", e);
            }
        });

        // not using S3Util.streamToS3 because we need to make sure the put completes before we return
        // the URL, as the client will go to it immediately.
        s3.putObject(bucket, String.format("%s.%s",key, format), pis, om);
    }

    /**
     * Download a grid in the selected format from S3, using presigned URLs
     * @param s3
     * @param bucket name of the bucket
     * @param filename both the key and the format
     * @param redirect
     * @param res
     * @return
     */
    public static Object downloadFromS3(AmazonS3 s3, String bucket, String filename, boolean redirect, Response res){
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(bucket, filename);
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);
        URL url = s3.generatePresignedUrl(presigned);

        if (redirect) {
            res.type("text/plain"); // override application/json
            res.redirect(url.toString());
            res.status(302); // temporary redirect, this URL will soon expire
            return null;
        } else {
            return new WrappedURL(url.toString());
        }
    }
}
