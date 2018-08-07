package com.conveyal.taui;

import com.conveyal.taui.models.AddTripPattern;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.Region;
import com.conveyal.taui.persistence.Persistence;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;

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
            Files.move(Paths.get("analysis.properties"), Paths.get("analysis.properties.original"),
                    StandardCopyOption.REPLACE_EXISTING);
        }

        // copy test version of analysis.properties to use in starting up server
        LOG.info("moving test analysis.properties file");
        Files.copy(
            Paths.get("analysis.properties.test"),
            Paths.get("analysis.properties"),
            StandardCopyOption.REPLACE_EXISTING
        );

        // drop the database to start fresh
        LOG.info("dropping test database");
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase(AnalysisServerConfig.databaseName);
        db.drop();

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

        // load database with some sample data
        LOG.info("loading test database with sample data");
        String accessGroup = "OFFLINE";

        Region regionWithProjects = new Region();
        regionWithProjects.accessGroup = accessGroup;
        regionWithProjects.name = "test-region-with-projects";

        Persistence.regions.create(regionWithProjects);

        Region regionWithNoProjects = new Region();
        regionWithNoProjects.accessGroup = accessGroup;
        regionWithNoProjects.name = "test-region-with-no-projects";

        Persistence.regions.create(regionWithNoProjects);

        Project projectWithModications = new Project();
        projectWithModications.accessGroup = accessGroup;
        projectWithModications.name = "project-with-modifications";
        projectWithModications.regionId = regionWithProjects._id;

        Persistence.projects.create(projectWithModications);

        Project projectWithNoModications = new Project();
        projectWithNoModications.accessGroup = accessGroup;
        projectWithNoModications.name = "project-with-no-modifications";
        projectWithNoModications.regionId = regionWithProjects._id;

        Persistence.projects.create(projectWithNoModications);

        AddTripPattern modificationWithTimetables = new AddTripPattern();
        modificationWithTimetables.accessGroup = accessGroup;
        modificationWithTimetables.name = "modification-with-timetables";
        modificationWithTimetables.projectId = projectWithModications._id;
        AddTripPattern.Timetable timetable = new AddTripPattern.Timetable();
        timetable.name = "weekday";
        timetable.startTime = 12345;
        timetable.endTime = 23456;
        timetable.headwaySecs = 1234;
        timetable.monday = true;
        timetable.tuesday = true;
        timetable.wednesday = true;
        timetable.thursday = true;
        timetable.friday = true;
        timetable.saturday = false;
        timetable.sunday = false;
        modificationWithTimetables.timetables = Arrays.asList(timetable);

        Persistence.modifications.create(modificationWithTimetables);

        AddTripPattern modificationWithNoTimetables = new AddTripPattern();
        modificationWithNoTimetables.accessGroup = accessGroup;
        modificationWithNoTimetables.name = "modification-with-no-timetables";
        modificationWithNoTimetables.projectId = projectWithModications._id;
        modificationWithNoTimetables.timetables = new ArrayList<>();

        Persistence.modifications.create(modificationWithNoTimetables);

        setUpIsDone = true;
    }
}
