package voldemort.predicatedetection.debug;

import voldemort.store.stats.RequestCounter;

/**
 * Created by duongnn on 2/11/18.
 */
// shared variables between functions
public class ExperimentVariables{
    // configuration for RequestCounter
    // copied from StoreStats.java
    private static final boolean useHistogram = true;
    private static final long timeWindow = 60000;

    // setup RequestCounter at application level to have closer observation of the benefit
    // of eventual consistency + monitoring vs sequential consistency
    // at the perspective of client apps.
    public static RequestCounter appCounterForPut = new RequestCounter("clientApp-counter-for-put", timeWindow, useHistogram);
    public static RequestCounter appCounterForGet = new RequestCounter("clientApp-counter-for-get", timeWindow, useHistogram);

    public static long operationCount;
    public static long numberOfOps;
    public static long mutableNumberOfOps;

    public static long numberOfNodesProcessed;

    public static String experimentStartDateTime;

    public static long experimentStartNs;
    public static long experimentStopNs;

    public static int runId;
    public static String serverIp;

    public static Thread mainClientThread;

    public static long timeForLocks;
    public static long timeForComputation;

    // duong debug
    public static long insufficientOperationalNodesExceptionCount;

    // for clientgraph coloring
    public static int[] myTaskAssignment = new int[2];
    public static int currentTaskId;
    public static int numberOfTasks;

    // duong debug
    public static int numberOfBorderNodes = 0;
    public static int numberOfLocks = 0;

}

