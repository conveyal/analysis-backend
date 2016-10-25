package com.conveyal.taui.util;

import com.conveyal.geojson.GeoJsonModule;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.r5.model.json_serialization.JavaLocalDateSerializer;
import com.conveyal.taui.models.JsonViews;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by matthewc on 2/9/16.
 */
public class JsonUtil {
    public static ObjectMapper objectMapper = getObjectMapper(JsonViews.Api.class);

    public static ObjectMapper getObjectMapper (Class view) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GeoJsonModule());
        objectMapper.registerModule(JavaLocalDateSerializer.makeModule());
        objectMapper.setConfig(objectMapper.getSerializationConfig().withView(view));
        return objectMapper;
    }
}
