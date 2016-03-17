package com.conveyal.taui.controllers;

import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema;
import com.conveyal.gtfs.model.FeedInfo;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema.feedType;
import static com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema.stringArg;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;
import static com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema.string;
import static spark.Spark.get;

/**
 * GraphQL interface to scenario editing tools. For now it just wraps the GTFS API graphql response with a bundle object.
 */
public class GraphQLController {
    public static Object handleQuery (Request req, Response res) throws IOException {
        Map<String, Object> variables = JsonUtil.objectMapper.readValue(req.queryParams("variables"), new TypeReference<Map<String, Object>>() { });
        ExecutionResult er = new GraphQL(schema).execute(req.queryParams("query"), null, null, variables);
        List<GraphQLError> errs = er.getErrors();
        if (!errs.isEmpty()) {
            res.status(400);
            return errs;
        }
        else {
            return er.getData();
        }
    }

    static GraphQLObjectType bundleType = newObject()
            .name("bundle")
            .field(string("id"))
            .field(string("name"))
            .field(newFieldDefinition()
                    .name("feeds")
                    .type(new GraphQLList(feedType))
                    .dataFetcher(GraphQLController::fetchFeeds)
                    .build()
            )
            .build();

    private static GraphQLObjectType bundleQuery = newObject()
            .name("bundleQuery")
            .field(newFieldDefinition()
                    .name("bundle")
                    .type(bundleType)
                    .argument(stringArg("bundle_id"))
                    .dataFetcher(GraphQLController::fetchBundle)
                    .build()
            )
            .build();

    public static GraphQLSchema schema = GraphQLSchema.newSchema().query(bundleQuery).build();

    private static Bundle fetchBundle(DataFetchingEnvironment environment) {
        String id = environment.getArgument("bundle_id");
        return Persistence.bundles.get(id);
    }

    private static List<FeedInfo> fetchFeeds(DataFetchingEnvironment environment) {
        Bundle bundle = (Bundle) environment.getSource();

        return bundle.feeds.stream()
                .map(summary -> ApiMain.feedSources.get(summary.feedId))
                .map(fs -> {
                    FeedInfo ret;
                    if (fs.feed.feedInfo.size() > 0) ret = fs.feed.feedInfo.values().iterator().next();
                    else {
                        ret = new FeedInfo();
                    }

                    if (ret.feed_id == null || "NONE".equals(ret.feed_id)) {
                        ret = ret.clone();
                        ret.feed_id = fs.feed.feedId;
                    }

                    return ret;
                })
                .collect(Collectors.toList());
    }

    public static void register () {
        get("/graphql", GraphQLController::handleQuery, JsonUtil.objectMapper::writeValueAsString);
    }
}
