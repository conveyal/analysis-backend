package com.conveyal.taui;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Wrapper around two ExecutorServices, making them accessible JVM-wide. The two fields here allow a "heavy" executor
 * (for slow tasks) and a "light" executor (for fast tasks, the "passing lane"), each with a number of threads that
 * can be limited in the main analysis.properties configuration file to help limit heavy concurrent operations.
 */
public abstract class ExecutorServices {
    public static final ExecutorService light = Executors.newFixedThreadPool(AnalysisServerConfig.lightThreads);
    public static final ExecutorService heavy = Executors.newFixedThreadPool(AnalysisServerConfig.heavyThreads);
}
