package com.conveyal.taui;

import org.junit.BeforeClass;
import org.junit.Test;

import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class AnalysisServerStaticFilesTest extends AnalysisServerTest {
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
        AnalysisServerTest.setUp();
        setUpIsDone = true;
    }

    /**
     * Make sure the index page can load.
     */
    @Test
    public void canReturnHtml() {
        String html = given()
            .port(7070)
            .get("/")
        .then()
            .body(containsString("Conveyal Analysis"))
        .extract().response().asString();
        assertThat(html, matchesSnapshot());
    }
}
