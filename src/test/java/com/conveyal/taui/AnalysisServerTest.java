package com.conveyal.taui;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

/**
 * A few tests of some basic routes of the server.
 * This relies on the configuration file in the project root being a local (offline=true) configuration.
 */
public class AnalysisServerTest {

    /**
     * Prepare and start a testing-specific web server
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        // Start a server using the configuration in the root of the project.
        String[] args = {};
        AnalysisServer.main(args);
    }

    /**
     * Make sure the index page can load.
     */
    @Test
    public void canReturnHtml() {
        given()
            .port(7070)
            .get("/")
        .then()
            .body(containsString("Conveyal Analysis"));
    }
}
