package com.conveyal.taui;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.response.Response;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    /**
     * Parse a json string into an unmapped JsonNode object
     */
    public static JsonNode parseJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    /**
     * Helper to return the relative path to a test resource file
     *
     * @param fileName
     * @return
     */
    public static String getResourceFileName(String fileName) {
        return String.format("./src/test/resources/%s", fileName);
    }

    /**
     * Recursively removes all specified keys.  This is used to make reliable snapshots.
     */
    public static void removeKeysAndValues (JsonNode json, String[] keysToRemove) {
        // remove key/value pairs that are likely to have different values each test run
        for (String key : keysToRemove) {
            if (json.has(key)) {
                ((ObjectNode) json).remove(key);
            }
        }

        // iterate through either the keys or array elements of this JsonObject/JsonArray
        if (json.isArray() || json.isObject()) {
            for (JsonNode nextNode : json) {
                removeKeysAndValues(nextNode, keysToRemove);
            }
        }
    }

    /**
     * Recursively removes all dynamically generated keys in order to perform reliable snapshots
     */
    public static void removeDynamicValues(JsonNode json) {
        removeKeysAndValues(json, new String[]{"_id", "createdAt", "nonce", "updatedAt"});
    }

    /**
     * An assertion to make sure that a given objectId is present in at least one of the objects in a response that is
     * supposed to contain a list of objects.
     *
     * This function should be passed a rest assured object built up until the point of calling .then()
     */
    public static void assertObjectIdInResponse(Response response, String objectId) {
        assertObjectIdInResponsePresence(response, objectId, true);
    }

    /**
     * An assertion to make sure that a given objectId is not present in any of the objects in a response that is
     * supposed to contain a list of objects
     *
     * This function should be passed a rest assured object built up until the point of calling .then()
     */
    public static void assertObjectIdNotInResponse(Response response, String objectId) {
        assertObjectIdInResponsePresence(response, objectId, false);
    }

    /**
     * Helper method to do the parsing and checking of whether an object with a givenn ObjectId is present or not in a
     * response that contains a list of objects
     */
    public static void assertObjectIdInResponsePresence(Response response, String objectId, boolean shouldBePresent) {
        ObjectWithId[] objects = response.then()
            .extract()
            .as(ObjectWithId[].class);

        boolean foundObject = false;
        for (ObjectWithId object : objects) {
            if (object._id.equals(objectId)) foundObject = true;
        }
        assertThat(foundObject, equalTo(shouldBePresent));
    }

    public static JsonNode createRegion () throws IOException {
        return parseJson(
            given()
                .port(7070)
                .contentType("multipart/form-data")
                .multiPart(
                    "region",
                    "{\"name\":\"test-create\",\"description\":\"test-description\",\"bounds\":{\"north\":37.05956083025126,\"south\":37.04421668967971,\"east\":-122.06634521484376,\"west\":-122.08282470703126}}"
                )
                .multiPart(
                    "customOpenStreetMapData",
                    new File(getResourceFileName("felton.pbf"))
                )
                .post("api/region")
            .then()
                .extract()
                .response()
                .asString()
        );
    }

    /**
     * Zip files in a folder into a temporary zip file
     */
    public static String zipFolderFiles(String folderName) throws IOException {
        // create temporary zip file
        File tempFile = File.createTempFile("temp-gtfs-zip-", ".zip");
        tempFile.deleteOnExit();
        String tempFilePath = tempFile.getAbsolutePath();

        // create a zip output stream for adding files to
        ZipOutputStream zipFile = new ZipOutputStream(new FileOutputStream(tempFilePath));

        // recursively add files from a folder to the zip file
        String fullFolderPath = getResourceFileName(folderName);
        compressDirectoryToZipfile(fullFolderPath, fullFolderPath, zipFile);
        IOUtils.closeQuietly(zipFile);

        return tempFilePath;
    }

    private static void compressDirectoryToZipfile(
        String rootDir,
        String sourceDir,
        ZipOutputStream out
    ) throws IOException {
        for (File file : new File(sourceDir).listFiles()) {
            if (file.isDirectory()) {
                compressDirectoryToZipfile(rootDir, sourceDir + File.separator + file.getName(), out);
            } else {
                ZipEntry entry = new ZipEntry(sourceDir.replace(rootDir, "") + file.getName());
                out.putNextEntry(entry);

                FileInputStream in = new FileInputStream(file.getAbsolutePath());
                IOUtils.copy(in, out);
                IOUtils.closeQuietly(in);
            }
        }
    }

    /**
     * Helper class for parsing json objects with the _id key.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ObjectWithId {
        public String _id;

        public ObjectWithId() {}
    }
}
