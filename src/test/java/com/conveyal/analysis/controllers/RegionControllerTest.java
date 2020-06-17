package com.conveyal.analysis.controllers;

import com.conveyal.analysis.AnalysisServerTest;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.conveyal.analysis.TestUtils.createRegion;
import static com.conveyal.analysis.TestUtils.objectIdInResponse;
import static com.conveyal.analysis.TestUtils.removeDynamicValues;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RegionControllerTest {
    private static boolean setUpIsDone = false;

    /**
     * Prepare and start a testing-specific web server
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        if (setUpIsDone) {
            return;
        }

        // start server if it isn't already running
        AnalysisServerTest.setUp();

        setUpIsDone = true;
    }

    @Test
    public void canCreateReadAndDeleteARegion () throws IOException {
        // create a region
        JsonNode json = createRegion();
        String regionId = json.get("_id").asText();

        // verify that the created region data matches the snapshot
        canCreateRegion(json);

        // verify region was created by calling the get region api endpoint and asserting that the returned object
        // contains the created region's ID
        given()
            .port(7070)
            .get("api/region/" + regionId)
        .then()
            .body("_id", equalTo(regionId));

        // delete the region and assert that the deleted object is returned with its ID
        given()
            .port(7070)
            .delete("api/region/" + regionId)
        .then()
            .body("_id", equalTo(regionId));

        // verify that the region was deleted by calling the get all regions api endpoint and making sure that the
        // created region's ID is not present in any region
        assertThat(
            objectIdInResponse(
                given()
                    .port(7070)
                    .get("api/region"),
                regionId
            ),
            equalTo(false)
        );
    }

    /**
     * Assert creation of region in this method, so it gets a proper snapshot name.
     */
    private void canCreateRegion(JsonNode json) {
        removeDynamicValues(json);
        assertThat(json, matchesSnapshot());
    }
}
