package com.conveyal.taui.persistence;

import com.conveyal.osmlib.OSM;
import com.conveyal.osmlib.OSMCache;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.models.Bounds;
import com.conveyal.taui.util.HttpUtil;
import com.google.common.io.ByteStreams;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;

/**
 * Manages storing OSM data in S3.
 */
public class OSMPersistence {
    public static final OSMCache cache =
            new OSMCache(AnalysisServerConfig.offline ? null : AnalysisServerConfig.bundleBucket, new File(AnalysisServerConfig.localCache));

    public static OSM retrieveOSMFromVexForBounds(Bounds bounds, String key) throws Exception {
        File temporaryFile = File.createTempFile("osm", ".pbf");
        String url = String.format(Locale.US, "%s/%f,%f,%f,%f.pbf", AnalysisServerConfig.vexUrl,
                bounds.south,
                bounds.west,
                bounds.north,
                bounds.east);

        HttpGet get = new HttpGet(url);
        CloseableHttpResponse res = null;
        try {
            res = HttpUtil.httpClient.execute(get);

            if (res.getStatusLine().getStatusCode() != 200) {
                throw AnalysisServerException.Unknown("Could not retrieve OSM. " + res.getStatusLine());
            }

            InputStream is = res.getEntity().getContent();
            OutputStream fos = new BufferedOutputStream(new FileOutputStream(temporaryFile));
            ByteStreams.copy(is, fos);
            fos.close();
            is.close();
            EntityUtils.consume(res.getEntity());

            cache.put(key, temporaryFile);
            temporaryFile.delete();
        } finally {
            if (res != null) res.close();
        }

        return cache.get(key);
    }
}
