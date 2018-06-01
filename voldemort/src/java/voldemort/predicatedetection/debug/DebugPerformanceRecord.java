package voldemort.predicatedetection.debug;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Vector;

/**
 * This class help us to record the throughput/latency of put and get requests
 * Created by duongnn on 9/14/17.
 */
public class DebugPerformanceRecord {

    // incrementStep helps us the reduce the size of output file written by DebugPerformanceRecord
    // instead of writing out every value recorded, we just write out values at specific intervals,
    // because we (when combing results from different servers) do not use all the values
    // But in case we want every values to be written out, set incrementStep = 0
    private int incrementStep;

    private Vector<DebugRecordEntry> recordVector;

    public DebugPerformanceRecord(){
        recordVector = new Vector<>(10000000);
        setIncrementStep(1000);
    }


    public void setIncrementStep(int incStep){
        incrementStep = incStep;
    }
    public int getIncrementStep(){
        return incrementStep;
    }
    public long getFirstTimestamp(){
        if(recordVector.size() == 0)
            return 0;

        return recordVector.firstElement().getTimeMs();
    }
    public long getLastTimestamp(){
        if(recordVector.size() == 0)
            return 0;

        return recordVector.lastElement().getTimeMs();
    }

    public void recordPerformance(long timeMs, double throughputOfPut, double latencyOfPut, double throughputOfGet, double latencyOfGet){
        recordVector.addElement(new DebugRecordEntry(timeMs, throughputOfPut, latencyOfPut, throughputOfGet, latencyOfGet));
    }

    // write both put and get information onto the same file

    public void writeDataToFile(String prefixName){
        String getOutFile = prefixName + ".txt";

        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter(getOutFile), true);

            int numbOfRecords = recordVector.size();

            if(numbOfRecords == 0) {
                //printWriter.printf("# recordVector is empty");

                // Duong debug
                System.out.println(" recordVector is empty");

                // recordVector is empty
                // since combine measurement program assume its input file not empty,
                // we just write 0 values
                printWriter.printf("%,20d  %15.2f %15.6f %15.2f %15.6f %n",
                        System.currentTimeMillis(),
                        0.0, 0.0, 0.0, 0.0);

                printWriter.close();

                return;
            }
            // now instead of writing out every values of the vectors, we just write at interval
            // because other values are not used any way

            // write comments
//            printWriter.printf("# timestamp %n");
//            printWriter.printf("# PUT throughput %n");
//            printWriter.printf("# PUT latency %n");
//            printWriter.printf("# GET throughput %n");
//            printWriter.printf("# GET latency %n");
//            printWriter.printf("# PUT and GET throughput %n");
//            printWriter.printf("%n%n");

            // write out the first element
            long intervalStartTimeMs;
            printWriter.printf("%,20d  %15.2f %15.6f %15.2f %15.6f %n",
                    recordVector.elementAt(0).getTimeMs(),
                    recordVector.elementAt(0).getThroughputOfPut(),
                    recordVector.elementAt(0).getLatencyOfPut(),
                    recordVector.elementAt(0).getThroughputOfGet(),
                    recordVector.elementAt(0).getLatencyOfGet());

            intervalStartTimeMs = recordVector.elementAt(0).getTimeMs();
            for(int i = 1; i < numbOfRecords; i++){
                // only write out if the timestamp is in next interval,
                // i.e. timestamp >= intervalStartTimeMs + incrementStep
                long currentTimeMs = recordVector.elementAt(i).getTimeMs();
                if(currentTimeMs >= intervalStartTimeMs + getIncrementStep()) {
                    printWriter.printf("%,20d  %15.2f %15.6f %15.2f %15.6f %n",
                            recordVector.elementAt(i).getTimeMs(),
                            recordVector.elementAt(i).getThroughputOfPut(),
                            recordVector.elementAt(i).getLatencyOfPut(),
                            recordVector.elementAt(i).getThroughputOfGet(),
                            recordVector.elementAt(i).getLatencyOfGet());

                    // update new intervalStartTimeMs
                    if(incrementStep > 0) {
                        while (intervalStartTimeMs + incrementStep <= currentTimeMs) {
                            intervalStartTimeMs += incrementStep;
                        }
                    }else{
                        // in case incrementStep = 0
                        intervalStartTimeMs = currentTimeMs;
                    }
                }
            }

            printWriter.close();

        }catch(IOException e){
            System.out.println(" DebugPerformanceRecord.writeDataToFile: ERROR");
            System.out.println(e.getMessage());
        }

    }

//    // write recorded data to file, one for put, one for get
//    public static void writeSeparateDataToFile(String prefixName){
//        writePutDataToFile(prefixName);
//        writeGetDataToFile(prefixName);
//    }
//
//    // write put data to file
//    public static void writePutDataToFile(String prefixName){
//        String putOutFile = prefixName + ".put.txt";
//
//        try {
//            PrintWriter printWriter = new PrintWriter(new FileWriter(putOutFile), true);
//
//            int numOfPuts = throughputPutVector.size();
//
////            for(int i = 0; i < numOfPuts; i++){
////                printWriter.printf("%,20d  %15.2f %15.6f %n",
////                                    throughputPutVector.elementAt(i).getTimeMs(),
////                                    throughputPutVector.elementAt(i).getMeasurement(),
////                                    latencyPutVector.elementAt(i).getMeasurement());
////            }
//
//            // now instead of writing out every values of the vectors, we just write at interval
//            // because other values are not used any way
//
//            // write out the first element
//            long intervalStartTimeMs;
//            printWriter.printf("%,20d  %15.2f %15.6f %n",
//                                throughputPutVector.elementAt(0).getTimeMs(),
//                                throughputPutVector.elementAt(0).getMeasurement(),
//                                latencyPutVector.elementAt(0).getMeasurement());
//
//            intervalStartTimeMs = throughputPutVector.elementAt(0).getTimeMs();
//            for(int i = 1; i < numOfPuts; i++){
//                // only write out if the timestamp is in next interval,
//                // i.e. timestamp >= intervalStartTimeMs + incrementStep
//                long currentTimeMs = throughputPutVector.elementAt(i).getTimeMs();
//                if(currentTimeMs >= intervalStartTimeMs + getIncrementStep()) {
//                    printWriter.printf("%,20d  %15.2f %15.6f %n",
//                            currentTimeMs,
//                            throughputPutVector.elementAt(i).getMeasurement(),
//                            latencyPutVector.elementAt(i).getMeasurement());
//
//                    // update new intervalStartTimeMs
//                    if(incrementStep > 0) {
//                        while (intervalStartTimeMs + incrementStep <= currentTimeMs) {
//                            intervalStartTimeMs += incrementStep;
//                        }
//                    }else{
//                        // in case incrementStep = 0
//                        intervalStartTimeMs = currentTimeMs;
//                    }
//                }
//            }
//
//            printWriter.close();
//
//        }catch(IOException e){
//            System.out.println(" DebugPerformanceRecord.writePutDataToFile: ERROR");
//            System.out.println(e.getMessage());
//        }
//    }
//
//    // write get data to file
//    public static void writeGetDataToFile(String prefixName){
//        String getOutFile = prefixName + ".get.txt";
//
//        try {
//            PrintWriter printWriter = new PrintWriter(new FileWriter(getOutFile), true);
//
//            int numOfGets = throughputGetVector.size();
//
//            // now instead of writing out every values of the vectors, we just write at interval
//            // because other values are not used any way
//
//            // write out the first element
//            long intervalStartTimeMs;
//            printWriter.printf("%,20d  %15.2f %15.6f %n",
//                    throughputGetVector.elementAt(0).getTimeMs(),
//                    throughputGetVector.elementAt(0).getMeasurement(),
//                    latencyGetVector.elementAt(0).getMeasurement());
//
//            intervalStartTimeMs = throughputGetVector.elementAt(0).getTimeMs();
//            for(int i = 1; i < numOfGets; i++){
//                // only write out if the timestamp is in next interval,
//                // i.e. timestamp >= intervalStartTimeMs + incrementStep
//                long currentTimeMs = throughputGetVector.elementAt(i).getTimeMs();
//                if(currentTimeMs >= intervalStartTimeMs + getIncrementStep()) {
//                    printWriter.printf("%,20d  %15.2f %15.6f %n",
//                            currentTimeMs,
//                            throughputGetVector.elementAt(i).getMeasurement(),
//                            latencyGetVector.elementAt(i).getMeasurement());
//
//                    // update new intervalStartTimeMs
//                    if(incrementStep > 0) {
//                        while (intervalStartTimeMs + incrementStep <= currentTimeMs) {
//                            intervalStartTimeMs += incrementStep;
//                        }
//                    }else{
//                        // in case incrementStep = 0
//                        intervalStartTimeMs = currentTimeMs;
//                    }
//                }
//            }
//
//            printWriter.close();
//
//        }catch(IOException e){
//            System.out.println(" DebugPerformanceRecord.writeGetDataToFile: ERROR");
//            System.out.println(e.getMessage());
//        }
//    }

    boolean isNaN(double x){
        return x != x;
    }

}
