package com.conveyal.taui.grids;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.persistence.FilePersistence;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Class with methods to write grids to specified formats, upload to S3, and direct clients to download them.
 *
 * Created by ansoncfit on 27-Nov-17, adapting code created by matthewc on 21-Oct-16
 */
public abstract class GridExporter {
    public enum Format {
        GRID, PNG, TIFF
    }

    public static Format format (String f) {
        return Format.valueOf(f.toUpperCase());
    }

    /**
     * Writes a grid to S3 with requested format
     *
     * @param grid grid to write
     * @param bucket name of the bucket
     * @param key
     * @param format allowable formats include grid (Conveyal flat binary format), png, and tiff.
     * @throws IOException
     */
    public static void write(Grid grid, String bucket, String key, Format format) throws IOException {
        File s3file = FilePersistence.in.create(bucket, key);
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
        fop.close();

        FilePersistence.in.put(bucket, key, s3file, om);
    }
}
