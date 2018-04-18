package com.conveyal.taui;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Shared unbounded Thread Executor for the entire server to help limit heavy concurrent operations
 */
public class ThreadPool {
    public static final ExecutorService executor = Executors.newFixedThreadPool(AnalysisServerConfig.maxThreads);
    public static void run (Runnable r) {
        executor.execute(r);
    }
}
