package com.conveyal.taui.controllers;

import com.conveyal.taui.AnalysisServerTest;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static com.conveyal.taui.TestUtils.assertObjectIdNotInResponse;
import static com.conveyal.taui.TestUtils.createRegion;
import static com.conveyal.taui.TestUtils.parseJson;
import static com.conveyal.taui.TestUtils.removeDynamicValues;
import static com.conveyal.taui.TestUtils.removeKeysAndValues;
import static com.conveyal.taui.TestUtils.zipFolderFiles;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BundleControllerTest {
    private static final Logger LOG = LoggerFactory.getLogger(BundleControllerTest.class);
    private static boolean setUpIsDone = false;
    private static String regionId;
    private static String simpleGtfsZipFileName;

    /**
     * Prepare and start a testing-specific web server and create a region for the bundle to be uploaded to
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        if (setUpIsDone) {
            return;
        }

        // start server if it isn't already running
        AnalysisServerTest.setUp();

        // create a region
        regionId = createRegion().get("_id").asText();

        // zip up the gtfs folder
        simpleGtfsZipFileName = zipFolderFiles("fake-agency");

        setUpIsDone = true;
    }

    @Test
    public void canCreateReadAndDeleteABundle () throws IOException {
        // create the bundle
        JsonNode json = parseJson(
            given()
                .port(7070)
                .contentType("multipart/form-data")
                .multiPart("Name", "test-bundle")
                .multiPart("regionId", regionId)
                .multiPart("files", new File(simpleGtfsZipFileName))
                .post("api/bundle")
            .then()
                .extract()
                .response()
                .asString()
        );
        String bundleId = json.get("_id").asText();

        // assert bundle was created properly
        canCreateBundle(json);

        // verify that the bundle can be fetched
        given()
            .port(7070)
            .get("api/bundle/" + bundleId)
        .then()
            .body("_id", equalTo(bundleId));

        // delete the bundle and verify that the bundle was returned
        given()
            .port(7070)
            .delete("api/bundle/" + bundleId)
        .then()
            .body("_id", equalTo(bundleId));

        // verify the bundle no longer exists by fetching all bundles and verifying that the bundle is no longer present
        assertObjectIdNotInResponse(
            given()
                .port(7070)
                .get("api/bundle"),
            bundleId
        );
    }

    /**
     * Assert creation of region in this method, so it gets a proper snapshot name.
     */
    private void canCreateBundle(JsonNode json) {
        assertThat(json.get("regionId").asText(), equalTo(regionId));
        removeDynamicValues(json);
        removeKeysAndValues(json, new String[]{"regionId"});
        assertThat(json, matchesSnapshot());
    }
}
