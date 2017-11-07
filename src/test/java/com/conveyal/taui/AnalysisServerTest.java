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
 * A few tests of some basic routes of the server
 */
public class AnalysisServerTest {
    private static File appConf = new File("./application.conf");
    private static File tempAppConf = new File("./application.conf-copied-during-test");

    /**
     * Prepare and start a testing-specific web server
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        // temporarily rename existing application.conf
        if (appConf.exists()) {
            Files.move(
                appConf.toPath(),
                tempAppConf.toPath()
            );
        }

        // copy testing-specific application.conf
        Files.copy(
            (new File("./src/test/resources/application.conf")).toPath(),
            appConf.toPath()
        );

        // start a server
        String[] args = {};
        AnalysisServer.main(args);
    }

    /**
     * Clean up various created application.conf files.
     * @throws IOException
     */
    @AfterClass
    public static void tearDown() throws IOException {
        // delete testing-specific application.conf
        appConf.delete();

        // if there already was an application.conf present, rename and move it back.
        if(tempAppConf.exists()) {
            Files.move(
                tempAppConf.toPath(),
                appConf.toPath()
            );
        }
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
