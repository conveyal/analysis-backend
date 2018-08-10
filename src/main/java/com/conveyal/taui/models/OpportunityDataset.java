package com.conveyal.taui.models;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.taui.grids.GridExporter;
import com.conveyal.taui.persistence.FilePersistence;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class OpportunityDataset extends Model {
    /** The human-readable name of the data source from which this came */
    public String sourceName;
    /** The unique id for this source set */
    public String sourceId;

    /** Bucket name */
    public String bucketName;

    /** Bounds */
    public int north;
    public int west;
    public int width;
    public int height;

    /** Total Opportunities */
    public double totalOpportunities;

    /**
     * For backwards compatibility.
     */
    @Deprecated
    public String key;

    /** The key on S3. */
    public String getKey (GridExporter.Format format) {
        return String.format("%s/%s.%s", this.regionId, this.key == null ? this._id : this.key, format.toString().toLowerCase());
    }

    public File putGrid (Grid grid, GridExporter.Format format) throws IOException {
        String key = this.getKey(format);
        File s3file = FilePersistence.in.create(bucketName, key);
        FileOutputStream fop = new FileOutputStream(s3file);
        ObjectMetadata om = new ObjectMetadata();

        if (GridExporter.Format.GRID.equals(format)) {
            om.setContentType("application/octet-stream");
            om.setContentEncoding("gzip");
            grid.write(new GZIPOutputStream(fop));
        } else if (GridExporter.Format.PNG.equals(format)) {
            om.setContentType("image/png");
            grid.writePng(fop);
        } else if (GridExporter.Format.TIFF.equals(format)) {
            om.setContentType("image/tiff");
            grid.writeGeotiff(fop);
        }
        fop.close();

        FilePersistence.in.put(bucketName, key, s3file, om);
        return s3file;
    }

    /** Region this dataset was uploaded in */
    public String regionId;
}
