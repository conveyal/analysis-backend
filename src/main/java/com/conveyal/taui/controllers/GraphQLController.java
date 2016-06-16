package com.conveyal.taui.controllers;

import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.graphql.RouteFetcher;
import com.conveyal.gtfs.api.graphql.StopFetcher;
import com.conveyal.gtfs.api.graphql.WrappedFeedInfo;
import com.conveyal.gtfs.api.graphql.WrappedGTFSEntity;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.gtfs.model.FeedInfo;
import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema.multiStringArg;
import static com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema.string;
import static com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema.routeType;
import static com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema.stopType;
import static com.conveyal.gtfs.api.graphql.GraphQLGtfsSchema.doublee;

import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.Scalars.GraphQLLong;
import static spark.Spark.get;

/**
 * GraphQL interface to scenario editing tools. For now it just wraps the GTFS API graphql response with a bundle object.
 */
public class GraphQLController {
    public static Object handleQuery (Request req, Response res) throws IOException {
        res.type("application/json");

        Map<String, Object> variables = JsonUtil.objectMapper.readValue(req.queryParams("variables"), new TypeReference<Map<String, Object>>() {
        });

        QueryContext context = new QueryContext();
        context.group = (String) req.attribute("group");

        ExecutionResult er = new GraphQL(schema).execute(req.queryParams("query"), null, context, variables);
        List<GraphQLError> errs = er.getErrors();
        if (!errs.isEmpty()) {
            res.status(400);
            return errs;
        } else {
            return er.getData();
        }
    }

    /** Special feed type that also includes checksum */
    public static GraphQLObjectType feedType = newObject()
            .name("feed")
            .field(string("feed_id"))
            .field(string("feed_publisher_name"))
            .field(string("feed_publisher_url"))
            .field(string("feed_lang"))
            .field(string("feed_version"))
            // We have a custom wrapped GTFS Entity type for FeedInfo that includes feed checksum
            .field(newFieldDefinition()
                    .name("checksum")
                    .type(GraphQLLong)
                    .dataFetcher(env -> ((WrappedFeedInfo) env.getSource()).checksum)
                    .build()
            )
            .field(newFieldDefinition()
                    .name("routes")
                    .type(new GraphQLList(routeType))
                    .argument(multiStringArg("route_id"))
                    .dataFetcher(RouteFetcher::forFeed)
                    .build()
            )
            .field(newFieldDefinition()
                    .name("stops")
                    .type(new GraphQLList(stopType))
                    .dataFetcher(StopFetcher::fromFeed)
                    .build()
            )
            .build();

    static GraphQLEnumType bundleStatus = newEnum()
            .name("status")
            .value("PROCESSING_GTFS", Bundle.Status.PROCESSING_GTFS)
            .value("PROCESSING_OSM", Bundle.Status.PROCESSING_OSM)
            .value("ERROR", Bundle.Status.ERROR)
            .value("DONE", Bundle.Status.DONE)
            .build();

    static GraphQLObjectType bundleType = newObject()
            .name("bundle")
            .field(string("id"))
            .field(string("name"))
            .field(newFieldDefinition()
                    .name("status")
                    .type(bundleStatus)
                    .dataFetcher((env) -> ((Bundle) env.getSource()).status)
                    .build()
            )
            .field(doublee("centerLat"))
            .field(doublee("centerLon"))
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
                    .type(new GraphQLList(bundleType))
                    .argument(multiStringArg("bundle_id"))
                    .dataFetcher(GraphQLController::fetchBundle)
                    .build()
            )
            .build();

    public static GraphQLSchema schema = GraphQLSchema.newSchema().query(bundleQuery).build();

    private static List<Bundle> fetchBundle(DataFetchingEnvironment environment) {
        List<String> id = environment.getArgument("bundle_id");
        QueryContext context = (QueryContext) environment.getContext();
        if (id != null) return Persistence.bundles.values().stream()
                .filter(b -> context.group.equals(b.group))
                .filter(b -> id.contains(b.id))
                .collect(Collectors.toList());

        else return new ArrayList<>(Persistence.bundles.values().stream()
                .filter(b -> context.group.equals(b.group))
                .collect(Collectors.toList())
        );
    }

    private static List<WrappedGTFSEntity<FeedInfo>> fetchFeeds(DataFetchingEnvironment environment) {
        Bundle bundle = (Bundle) environment.getSource();

        return bundle.feeds.stream()
                .map(summary -> {
                    FeedSource fs = ApiMain.getFeedSource(summary.id);

                    FeedInfo ret;
                    if (fs.feed.feedInfo.size() > 0) ret = fs.feed.feedInfo.values().iterator().next();
                    else {
                        ret = new FeedInfo();
                    }

                    if (ret.feed_id == null || "NONE".equals(ret.feed_id)) {
                        ret = ret.clone();
                        ret.feed_id = fs.feed.feedId;
                    }

                    return new WrappedFeedInfo(summary.id, ret, summary.checksum);
                })
                .collect(Collectors.toList());
    }

    public static void register () {
        get("/api/graphql", GraphQLController::handleQuery, JsonUtil.objectMapper::writeValueAsString);
    }

    /** Context for a graphql query. Currently contains auth info */
    public static class QueryContext {
        public String group;
    }
}
