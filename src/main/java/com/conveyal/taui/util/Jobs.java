package com.conveyal.taui.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Contains a shared ExecutorService.
 */
public class Jobs {
    public static ExecutorService service = Executors.newCachedThreadPool();
}
