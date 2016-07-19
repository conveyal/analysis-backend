package com.conveyal.taui.migrations;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.taui.AnalystConfig;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.persistence.Persistence;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Migrate bundles to individual feed storage.
 */
public class MigrateBundles {
    private static final Logger LOG = LoggerFactory.getLogger(MigrateBundles.class);

    public static void main (String... args) throws Exception {
        AmazonS3 s3 = new AmazonS3Client();

        File cacheDir = new File(AnalystConfig.localCache);
        cacheDir.mkdirs();
        ApiMain.initialize(AnalystConfig.bundleBucket, AnalystConfig.localCache);
        Persistence.initialize();

        for (Bundle bundle : Persistence.bundles.values()) {
            // try to load bundle from S3
            S3Object obj;
            try {
                obj = s3.getObject(AnalystConfig.bundleBucket, bundle.id + ".zip");
            } catch (AmazonServiceException e) {
                LOG.warn("Could not retrieve bundle {}, perhaps it is already migrated?", bundle.id, e);
                continue;
            }

            File bundleCache = File.createTempFile("bundle", ".zip");

            InputStream is = obj.getObjectContent();
            OutputStream os = new FileOutputStream(bundleCache);
            ByteStreams.copy(is, os);
            is.close();
            os.close();

            ZipFile zf = new ZipFile(bundleCache);

            if (bundle.feeds == null) {
                LOG.warn("Bundle {} has null feeds", bundle);
                continue;
            }

            bundle.feeds.stream().forEach(feed -> {
                if (feed.fileName == null) {
                    LOG.warn("Feed {} of bundle {} has null filename", feed.feedId, bundle);
                    return;
                }

                try {
                    File entryFile = File.createTempFile("gtfs", ".zip");
                    InputStream eis = zf.getInputStream(zf.getEntry(feed.fileName));
                    OutputStream eos = new FileOutputStream(entryFile);
                    ByteStreams.copy(eis, eos);
                    eis.close();
                    eos.close();

                    // feedsummary.id already populated by mongo migration
                    ApiMain.registerFeedSource(feed.id, entryFile);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
