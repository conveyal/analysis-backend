package com.conveyal.taui.util;

import static spark.Spark.halt;

/**
 * Created by trevorgerhardt on 9/12/17.
 */
public class SparkUtil {
    public static void haltWithJson(int code, String message) {
        halt(code, "{\"message\":\"" + message + "\"}");
    }
}
