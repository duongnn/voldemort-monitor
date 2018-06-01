package voldemort.predicatedetection.debug;

/**
 * Created by duongnn on 2/11/18.
 */
public class DebugPerformance {
    // performance record for either Voldemort client or server
    public static DebugPerformanceRecord voldemort = new DebugPerformanceRecord();

    // performance record for application
    public static DebugPerformanceRecord application = new DebugPerformanceRecord();

}
