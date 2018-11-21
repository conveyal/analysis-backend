package com.conveyal.taui.grids;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.util.WrappedURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Response;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URL;
import java.text.Normalizer;
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

    /** How long request URLs are good for */
    public static final int REQUEST_TIMEOUT_MSEC = 300 * 1000;

    public enum Format {
        GRID, PNG, TIFF
    }

    public static Format format (String f) {
        return Format.valueOf(f.toUpperCase());
    }

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
    public static boolean checkRedirectAndFormat(String redirectText, Format format){
        // FIXME replace string matching with enum type
        if (!Format.GRID.equals(format) && !Format.PNG.equals(format) && !Format.TIFF.equals(format)) {
            haltWithIncorrectFormat(format.toString());
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
    public static void writeToS3(Grid grid, AmazonS3 s3, String bucket, String key, Format format) throws IOException {
        File s3file = File.createTempFile(key, null);
        FileOutputStream fop = new FileOutputStream(s3file);
        ObjectMetadata om = new ObjectMetadata();

        if (Format.GRID.equals(format)) {
            om.setContentType("application/octet-stream");
            om.setContentEncoding("gzip");
            grid.write(new GZIPOutputStream(fop));
        } else if (Format.PNG.equals(format)) {
            om.setContentType("image/png");
            grid.writePng(fop);
        } else if (Format.TIFF.equals(format)) {
            om.setContentType("image/tiff");
            grid.writeGeotiff(fop);
        }

        PutObjectRequest por = new PutObjectRequest(bucket, key, s3file).withMetadata(om);
        s3.putObject(por);
        s3file.delete();
    }

    /**
     * Return a response to the client redirecting it to a grid in the selected format from S3, using presigned URLs.
     * If the browser does an automatic redirect, it sends our application's authorization headers to AWS S3 which
     * wreaks havoc with CORS etc. So we generally want to avoid an automatic redirect and just send the URL in the
     * response body, so the client code can re-issue the request manually with its choice of headers.
     *
     * If redirect is true, we generate a 302 redirect which will be handled automatically by the browser.
     * If it's false, we just return some JSON containing the target URL with a 200 OK code.
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
