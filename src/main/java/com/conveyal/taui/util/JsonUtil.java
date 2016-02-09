package com.conveyal.taui.util;

import com.conveyal.r5.common.JsonUtilities;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by matthewc on 2/9/16.
 */
public class JsonUtil {
    // just use r5 object mapper for now.
    public static ObjectMapper objectMapper = JsonUtilities.objectMapper;
}
