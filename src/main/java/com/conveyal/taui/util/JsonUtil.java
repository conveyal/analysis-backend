package com.conveyal.taui.util;

import com.conveyal.geojson.GeoJsonModule;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.r5.model.json_serialization.JavaLocalDateSerializer;
import com.conveyal.taui.models.JsonViews;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mongojack.internal.MongoJackModule;

public abstract class JsonUtil {


    public static ObjectMapper objectMapper = getObjectMapper(JsonViews.Api.class);

    public static ObjectMapper getObjectMapper (Class view) {
        return getObjectMapper(view, false);
    }

    public static ObjectMapper getObjectMapper(Class view, boolean configureMongoJack) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GeoJsonModule());
        objectMapper.registerModule(JavaLocalDateSerializer.makeModule());

        if (configureMongoJack) MongoJackModule.configure(objectMapper);

        // We removed a bunch of fields from ProfileRequests which are persisted to the DB
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        objectMapper.setConfig(objectMapper.getSerializationConfig().withView(view));
        return objectMapper;
    }

}
