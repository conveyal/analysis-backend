package com.conveyal.taui.persistence;

import com.conveyal.osmlib.OSMCache;
import com.conveyal.taui.AnalysisServerConfig;

import java.io.File;

/**
 * Manages storing OSM data in S3.
 */
public class OSMPersistence {
    public static final OSMCache cache =
            new OSMCache(AnalysisServerConfig.offline ? null : AnalysisServerConfig.bundleBucket, new File(AnalysisServerConfig.localCache));
}
