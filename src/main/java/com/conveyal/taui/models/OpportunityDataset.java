package com.conveyal.taui.models;

import com.conveyal.taui.grids.GridExporter;

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

    /** Region this dataset was uploaded in */
    public String regionId;
}
