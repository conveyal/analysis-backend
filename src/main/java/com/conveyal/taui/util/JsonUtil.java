package com.conveyal.taui.util;

import com.conveyal.geojson.GeoJsonModule;
import com.conveyal.r5.common.JsonUtilities;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by matthewc on 2/9/16.
 */
public class JsonUtil {
    public static ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.registerModule(new GeoJsonModule());
    }
}
