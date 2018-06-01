package voldemort.predicatedetection.debug;

/**
 * Created by duongnn on 10/19/17.
 */
public class DebugRecordEntry {
    private long timeMs;        // moment of measurement, in milliseconds
    private double throughputOfPut;   // average put throughput at this moment
    private double latencyOfPut;      // average put latency at this moment
    private double throughputOfGet;   // average get throughput
    private double latencyOfGet;      // average get latency

    DebugRecordEntry(long timeMs, double throughputOfPut, double latencyOfPut, double throughputOfGet, double latencyOfGet){

        this.timeMs = timeMs;

        this.throughputOfPut = throughputOfPut;
        this.latencyOfPut = latencyOfPut;

        this.throughputOfGet = throughputOfGet;
        this.latencyOfGet = latencyOfGet;
    }

    public long getTimeMs() {
        return timeMs;
    }

    public double getThroughputOfPut() {
        return throughputOfPut;
    }

    public double getLatencyOfPut() {
        return latencyOfPut;
    }

    public double getThroughputOfGet() {
        return throughputOfGet;
    }

    public double getLatencyOfGet() {
        return latencyOfGet;
    }

}
