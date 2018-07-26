package com.conveyal.taui.controllers;

import com.conveyal.taui.AnalysisServerTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;

public class TimetableControllerTest {
    private static final Logger LOG = LoggerFactory.getLogger(TimetableControllerTest.class);
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

        // populate
        setUpIsDone = true;
    }

    /**
     * Make sure the index page can load.
     */
    @Test
    public void canReturnTimetables() throws IOException {
        String jsonString = given()
            .port(7070)
            .get("/api/timetables")
        .then()
            .extract().response().asString();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonString);
        removeDynamicKeys(json);
        assertThat(json, matchesSnapshot());
    }

    /**
     * Removes all dynamically generated keys in order to perform reliable snapshots
     */
    private void removeDynamicKeys(JsonNode json) {
        // remove unwanted keys
        if (json.has("_id")) {
            ((ObjectNode) json).remove("_id");
        }

        // iterate through either the keys or array elements of this JsonObject/JsonArray
        if (json.isArray() || json.isObject()) {
            for (JsonNode nextNode : json) {
                removeDynamicKeys(nextNode);
            }
        }
    }
}