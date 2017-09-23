package com.conveyal.taui.persistence;

import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.models.Bookmark;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.models.JsonViews;
import com.conveyal.taui.models.AggregationArea;
import com.conveyal.taui.models.Model;
import com.conveyal.taui.models.Modification;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.models.Scenario;
import com.conveyal.taui.util.JsonUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import org.mongojack.JacksonDBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a single connection to MongoDB for the entire TAUI server process.
 */
public class Persistence {
    private static final Logger LOG = LoggerFactory.getLogger(Persistence.class);

    // TODO deprecated but needed for MongoJack
    private static MongoClient mongo;
    private static DB db;

    public static MongoMap<Modification> modifications;
    public static MongoMap<Scenario> scenarios;
    public static MongoMap<Bundle> bundles;
    public static MongoMap<Project> projects;
    public static MongoMap<RegionalAnalysis> regionalAnalyses;
    public static MongoMap<Bookmark> bookmarks;
    public static MongoMap<AggregationArea> aggregationAreas;

    public static void initialize () {
        LOG.info("Connecting to MongoDB...");
        // allow configurable db connection params
        if (AnalysisServerConfig.databaseUri != null) {
            MongoClientOptions.Builder builder = MongoClientOptions.builder()
                    .sslEnabled(true);
            mongo = new MongoClient(new MongoClientURI(AnalysisServerConfig.databaseUri, builder));
            LOG.info("Connecting to remote MongoDB instance...");
        }
        else {
            LOG.info("Connecting to local MongoDB instance...");
            mongo = new MongoClient();
        }

        db = mongo.getDB(AnalysisServerConfig.databaseName);

        modifications = getTable("modifications", Modification.class);
        scenarios = getTable("scenarios", Scenario.class);
        bundles = getTable("bundles", Bundle.class);
        projects = getTable("projects", Project.class);
        regionalAnalyses = getTable("regional-analyses", RegionalAnalysis.class);
        bookmarks = getTable("bookmarks", Bookmark.class);
        aggregationAreas = getTable("aggregationAreas", AggregationArea.class);
    }

    /** connect to a table using MongoJack */
    private static <V extends Model> MongoMap<V> getTable (String name, Class clazz) {
        DBCollection collection = db.getCollection(name);
        ObjectMapper om = JsonUtil.getObjectMapper(JsonViews.Db.class, true);
        JacksonDBCollection<V, String> coll = JacksonDBCollection.wrap(collection, clazz, String.class, om);
        return new MongoMap<>(coll);
    }
}
