package com.conveyal.taui;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * A few tests of some basic routes of the server.
 * This relies on the configuration file in the project root being a local (offline=true) configuration.
 */
public class AnalysisServerTest {
    private static final Logger LOG = LoggerFactory.getLogger(AnalysisServerTest.class);
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

        // see if analysis.properties exists before testing
        File analysisPropertiesFile = new File("analysis.properties");
        boolean analysisPropertiesFileExistedBeforeTesting = analysisPropertiesFile.exists();

        // copy actual analysis.properties so we don't overwrite it
        if (analysisPropertiesFileExistedBeforeTesting) {
            LOG.info("moving existing analysis.properties file");
            Files.move(Paths.get("analysis.properties"), Paths.get("analysis.properties.original"));
        }

        // copy test version of analysis.properties to use in starting up server
        LOG.info("moving test analysis.properties file");
        Files.copy(
            Paths.get("analysis.properties.test"),
            Paths.get("analysis.properties"),
            StandardCopyOption.REPLACE_EXISTING
        );

        // Start a server using the configuration in the root of the project.
        String[] args = {};
        AnalysisServer.main(args);

        // replace analysis.properties with original file
        if (analysisPropertiesFileExistedBeforeTesting) {
            LOG.info("moving back existing analysis.properties file");
            Files.move(
                Paths.get("analysis.properties.original"),
                Paths.get("analysis.properties"),
                StandardCopyOption.REPLACE_EXISTING
            );
        }

        setUpIsDone = true;
    }
}
