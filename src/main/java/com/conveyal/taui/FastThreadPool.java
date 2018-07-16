package com.conveyal.taui;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FastThreadPool extends ThreadPool {
    public static final ExecutorService executor = Executors.newFixedThreadPool(AnalysisServerConfig.slowThreads);
    public static void run (Runnable r) {
        executor.execute(r);
    }
}
