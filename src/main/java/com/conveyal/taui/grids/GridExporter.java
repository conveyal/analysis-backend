package com.conveyal.taui.grids;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.AnalysisServerException;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

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
     * Checks request parameters, ensuring that the requested download format is one of the allowable ones.
     *
     * @param format requested format
     * @return cleaned up version of redirectText
     */
    public static void checkFormat(Format format){
        if (!Format.GRID.equals(format) && !Format.PNG.equals(format) && !Format.TIFF.equals(format)) {
            haltWithIncorrectFormat(format.toString());
        }
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
     * In the future it could be possible to generate a 302 redirect which will be handled automatically by the browser.
     * For now just return some JSON containing the target URL with a 200 OK code.
     */
    public static JSONObject downloadFromS3(AmazonS3 s3, String bucket, String filename){
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(bucket, filename);
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);
        URL url = s3.generatePresignedUrl(presigned);

        JSONObject m = new JSONObject();
        m.put("url", url.toString());

        return m;
    }
}
