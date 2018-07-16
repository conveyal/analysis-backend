package com.conveyal.taui;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Shared unbounded Thread Executor for the entire server to help limit heavy concurrent operations
 */
public class ExecutorServices {
    public static final ExecutorService light = Executors.newFixedThreadPool(AnalysisServerConfig.lightThreads);
    public static final ExecutorService heavy = Executors.newFixedThreadPool(AnalysisServerConfig.heavyThreads);
}
