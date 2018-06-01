package predicatedetectionlib.monitor;

import java.io.PrintWriter;
import java.util.Vector;

/**
 * Created by duongnn on 3/28/18.
 */
public class Monitor {
    public static volatile boolean stopThreadNow;

    protected static int monitorId;

    protected static long epsilon; // monitor knowledge about epsilon
    protected static int numberOfServers;
    protected static int monitorPortNumber;
    protected static String monitorIp;
    protected static int runId;

    protected static long totalNumberOfGlobalPredicatesDetected = 0;

    protected static Vector<Long> historyResponseTime;
    protected static double averageResponseTime = 0;
    protected static long maxResponseTime = 0;
    protected static long minResponseTime = 0;

    protected static Vector<Long> historyPhysicalResponseTime;
    protected static double averagePhysicalResponseTime = 0;
    protected static long maxPhysicalResponseTime = 0;
    protected static long minPhysicalResponseTime = 0;

    public static PrintWriter outFile;

    public static long getEpsilon(){
        return epsilon;
    }

    public static int getNumberOfServers(){
        return numberOfServers;
    }

    public static int getMonitorPortNumber(){
        return monitorPortNumber;
    }

    public static String getMonitorIp(){
        return monitorIp;
    }

    public static int getMonitorId(){
        return monitorId;
    }

    static void updateResponseTimeStatistics(){
        long currentResponseTime;
        long currentPhysicalResponseTime;

        totalNumberOfGlobalPredicatesDetected = historyResponseTime.size();
        if(totalNumberOfGlobalPredicatesDetected == 0)
            return;

        averageResponseTime = 0;

        currentResponseTime = historyResponseTime.elementAt(0);
        maxResponseTime = minResponseTime = currentResponseTime;
        averageResponseTime += currentResponseTime;

        for(int i = 1; i < totalNumberOfGlobalPredicatesDetected; i++){
            currentResponseTime = historyResponseTime.elementAt(i);
            if(maxResponseTime < currentResponseTime)
                maxResponseTime = currentResponseTime;
            if(minResponseTime > currentResponseTime)
                minResponseTime = currentResponseTime;
            averageResponseTime += currentResponseTime;
        }

        averageResponseTime = averageResponseTime/ (1.0 * totalNumberOfGlobalPredicatesDetected);

        // for physical time
        averagePhysicalResponseTime = 0;

        currentPhysicalResponseTime = historyPhysicalResponseTime.elementAt(0);
        maxPhysicalResponseTime = minPhysicalResponseTime = currentPhysicalResponseTime;
        averagePhysicalResponseTime += currentPhysicalResponseTime;

        for(int i = 1; i < totalNumberOfGlobalPredicatesDetected; i++){
            currentPhysicalResponseTime = historyPhysicalResponseTime.elementAt(i);
            if(maxPhysicalResponseTime < currentPhysicalResponseTime)
                maxPhysicalResponseTime = currentPhysicalResponseTime;
            if(minPhysicalResponseTime > currentPhysicalResponseTime)
                minPhysicalResponseTime = currentPhysicalResponseTime;
            averagePhysicalResponseTime += currentPhysicalResponseTime;
        }

        averagePhysicalResponseTime = averagePhysicalResponseTime/ (1.0 * totalNumberOfGlobalPredicatesDetected);

    }

}
