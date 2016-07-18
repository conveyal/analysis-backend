package com.conveyal.taui.persistence;

import com.conveyal.osmlib.OSMCache;
import com.conveyal.taui.AnalystConfig;

import java.io.File;

/**
 * Manages storing OSM data in S3.
 */
public class OSMPersistence {
    public static final OSMCache cache =
            new OSMCache(AnalystConfig.offline ? null : AnalystConfig.bundleBucket, new File(AnalystConfig.localCache));
}
