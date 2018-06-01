//package predicatedetectionlib.monitor;
//
//import org.w3c.dom.Element;
//import predicatedetectionlib.common.*;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.*;
//import java.util.concurrent.LinkedBlockingQueue;
//import joptsimple.OptionParser;
//import joptsimple.OptionSet;
//import joptsimple.OptionSpec;
//import predicatedetectionlib.common.predicate.Predicate;
//
///**
// * SinglePredicateMonitor process
// * NIO client server is based on Crunchify.com
// *
// * The monitor has 2 threads
// *  One is candidate receiving which receives candidate message from servers.
// *  The other is candidate processing which process the received the candidate.
// * The first thread acts as the producer. The second thread acts as the consumer
// *
// * This monitor is designed for the case one monitor, one predicate, one process,
// * and the predicate is known in advance
// *
// * Created by duongnn on 5/27/17.
// *
// */
//
//public class SinglePredicateMonitor extends Monitor {
////    public static volatile boolean stopThreadNow;
//
//    // predicate under supervision of this monitor
//    private static Predicate monitorPredicate = null;
//
////    private static int monitorId;
////
////    private static long epsilon; // monitor knowledge about epsilon
////    private static int numberOfServers;
////    private static int monitorPortNumber;
////    private static int runId;
////
////    private static long totalNumberOfGlobalPredicatesDetected = 0;
////
////    protected static Vector<Long> historyResponseTime;
////    private static double averageResponseTime = 0;
////    private static long maxResponseTime = 0;
////    private static long minResponseTime = 0;
////
////    protected static Vector<Long> historyPhysicalResponseTime;
////    private static double averagePhysicalResponseTime = 0;
////    private static long maxPhysicalResponseTime = 0;
////    private static long minPhysicalResponseTime = 0;
////
////    public static PrintWriter outFile;
//
////    public static long getEpsilon(){
////        return epsilon;
////    }
////
////    public static int getNumberOfServers(){
////        return numberOfServers;
////    }
////
////    public static int getMonitorPortNumber(){
////        return monitorPortNumber;
////    }
////
////    public static Predicate getMonitorPredicate(){
////        return monitorPredicate;
////    }
////
////    public static int getMonitorId(){
////        return monitorId;
////    }
//
//    public static void main(String[] args){
//        // parsing main argument
//        OptionParser parser = new OptionParser();
//        OptionSpec<Integer> monitorIdOptionSpec = parser.accepts("monitor-id")
//                .withRequiredArg().ofType(Integer.class);
//        parser.accepts("monitor-input-arg-file")
//                .withRequiredArg();
//
//        OptionSet options = parser.parse(args);
//
//        monitorId = options.valueOf(monitorIdOptionSpec);
//        String monitorInputFileName = "../../voldemort-related/scripts/" + (String) options.valueOf("monitor-input-arg-file");
//
//        // open monitor input argument file to get the arguments
//        Vector<Element> monitorElementList = XmlUtils.getElementListFromFile(monitorInputFileName, "monitor");
//        Vector<Element> predicateElementList = XmlUtils.getElementListFromFile(monitorInputFileName, "predicate");
//
//        // sanity check
//        if(monitorId >= monitorElementList.size()){
//            System.out.println(" SinglePredicateMonitor.main() ERROR:");
//            System.out.println("   monitorId (" + monitorId + ") >= monitorElementList.size (" + monitorElementList.size() + ")");
//            System.exit(1);
//        }
//        if(predicateElementList.size() != monitorElementList.size()){
//            System.out.println(" SinglePredicateMonitor.main() ERROR:");
//            System.out.println("   predicateElementList.size (" + predicateElementList.size() +
//                    ") != monitorElementList.size (" + monitorElementList.size() + ")");
//            System.exit(1);
//        }
//
//        // getting arguments provided via input file
//        String[] mainArg = null;
//        for(int i = 0; i < monitorElementList.size(); i++){
//            Element monitorElement = monitorElementList.elementAt(i);
//            String idStr = monitorElement.getElementsByTagName("id").item(0).getTextContent();
//            String mainArgStr = monitorElement.getElementsByTagName("mainArg").item(0).getTextContent();
//
//            if(Integer.valueOf(idStr) == monitorId){
//                mainArg = mainArgStr.split("\\s+");
//                break;
//            }
//        }
//
//        // getting information of predicate associated with this monitor
//        for(int i = 0; i < predicateElementList.size(); i++){
//            Element predElement = predicateElementList.elementAt(i);
//            String idStr = predElement.getElementsByTagName("id").item(0).getTextContent();
//            if(monitorId == Integer.valueOf(idStr)){
//                monitorPredicate = XmlUtils.elementToPredicate(predElement);
//                break;
//            }
//        }
//
//        if(mainArg == null){
//            System.out.println(" SinglePredicateMonitor.main ERROR: mainArg is null");
//            System.exit(1);
//        }
//
//        // parsing arguments provided via input file
//        OptionSpec<Integer> numberOfServerOption = parser.accepts("number-of-servers", "number of servers")
//                .withRequiredArg()
//                .describedAs("number-of-servers")
//                .ofType(Integer.class);
//        OptionSpec<Long> epsilonOption = parser.accepts("monitor-epsilon", "value of Epsilon, the uncertainty window, in milliseconds")
//                .withOptionalArg()
//                .describedAs("monitor-epsilon")
//                .ofType(Long.class)
//                .defaultsTo(300000L); // --epsilon=some_number 300000 is 5 minutes
////        OptionSpec<Integer> monitorPortNumberOption = parser.accepts("monitor-port-number", "which port number the monitor is listening for messages")
////                .withOptionalArg()
////                .describedAs("monitor-port-number")
////                .ofType(Integer.class)
////                .defaultsTo(3308);
//        OptionSpec<String> outFileNameOption = parser.accepts("out-file-name", "out-file-name")
//                .withOptionalArg()
//                .ofType(String.class)
//                .defaultsTo("monitor_debug_out");
//        OptionSpec<Integer> runIdOptionSpec = parser.accepts("run-id", "sequence number of this run")
//                .withRequiredArg()
//                .ofType(Integer.class);
//
//        options = parser.parse(mainArg);
//
//        numberOfServers = options.valueOf(numberOfServerOption);
//        epsilon = options.valueOf(epsilonOption);
//        runId = options.valueOf(runIdOptionSpec);
//        String outFileName = options.valueOf(outFileNameOption) + "-run" + runId + "-monitor" + monitorId + ".txt";
//
////        monitorPortNumber = options.valueOf(monitorPortNumberOption);
//        monitorPortNumber = Integer.valueOf(monitorPredicate.getMonitorPortNumber());
//
//        try {
//            outFile = new PrintWriter(new FileWriter(outFileName, false));
//        }catch(IOException e){
//            System.out.println(e.getMessage());
//            System.out.println(" could not open file " + outFileName + " to write data");
//            System.exit(1);
//        }
//
//        System.out.println(monitorPredicate.toString(0));
//
//        // setting up shared blocking queues between producer and consumer
//
//        Vector<LinkedBlockingQueue<LocalSnapshot>> blockingQueueArray =
//                new Vector<>(numberOfServers);
//        for(int i = 0; i < numberOfServers; i++){
//            blockingQueueArray.addElement(new LinkedBlockingQueue<>());
//            //blockingQueueArray.elementAt(i) = new LinkedBlockingQueue<>(); // incorrect way to init
//        }
//
//        // initialize and start producer and consumer process
//        SinglePredicateCandidateReceiving producer = new SinglePredicateCandidateReceiving(blockingQueueArray);
//        SinglePredicateCandidateProcessing consumer = null;
//
//        switch(monitorPredicate.getType()){
//            case LINEAR:
//                consumer = new SinglePredicateCandidateProcessingLinear(blockingQueueArray);
//                break;
//            case SEMILINEAR:
//                consumer = new SinglePredicateCandidateProcessingSemiLinear(blockingQueueArray);
//
//                break;
//            default:
//                System.out.println(" SinglePredicateMonitor ERROR: unrecognized predicate type" + monitorPredicate.getType());
//                System.exit(1);
//        }
//
//
//        // start two processes
//        stopThreadNow = false;
//        new Thread(producer).start();
//        new Thread(consumer).start();
//
//        // add shutdown hook
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//
//            @Override
//            public void run() {
//
//                //Duong debug
//                System.out.println(" Receive addShutdownHook signal");
//                System.out.println(" Stoping candidate receiving and candidate processing threads");
//
//                stopThreadNow = true;
//
//                updateResponseTimeStatistics();
//
//                String summaryStr = String.format("\n") +
//                        String.format("# Total number of global predicates detected =  %,d %n", totalNumberOfGlobalPredicatesDetected) +
//                        String.format("#   Average response time =          %,.2f %n", averageResponseTime) +
//                        String.format("#   Max response time =              %,d %n", maxResponseTime) +
//                        String.format("#   Min reponse time =               %,d %n", minResponseTime) +
//                        String.format("# %n") +
//                        String.format("#   Average physical response time = %,.2f %n", averagePhysicalResponseTime) +
//                        String.format("#   Max physical response time =     %,d %n", maxPhysicalResponseTime) +
//                        String.format("#   Min physical reponse time =      %,d %n", minPhysicalResponseTime) +
//
//                        String.format("#%n");
//
//                System.out.println(summaryStr);
//
//                // write summary message to file
//                // this file differs from outFile which is mainly used for debugging
//                try {
//                    String monitorOutputFileName = "monitorOutputFile-run" + runId +
//                            "-monitor" + monitorId + ".txt";
//                    PrintWriter printWriter = new PrintWriter(new FileWriter(monitorOutputFileName), true);
//                    printWriter.printf("%s", summaryStr);
//
//                    // write out records of response time for figures
//                    for(int i = 0; i < totalNumberOfGlobalPredicatesDetected; i++){
//                        printWriter.printf(" %10d  %10d  %10d %n",
//                                i,
//                                historyResponseTime.elementAt(i),
//                                historyPhysicalResponseTime.elementAt(i));
//                    }
//                    printWriter.close();
//
//                    outFile.close();
//
//                }catch(IOException e){
//                    System.out.println(e.getMessage());
//                }
//
//            }
//        });
//    }
//
////    static void updateResponseTimeStatistics(){
////        long currentResponseTime;
////        long currentPhysicalResponseTime;
////
////        totalNumberOfGlobalPredicatesDetected = historyResponseTime.size();
////        if(totalNumberOfGlobalPredicatesDetected == 0)
////            return;
////
////        averageResponseTime = 0;
////
////        currentResponseTime = historyResponseTime.elementAt(0);
////        maxResponseTime = minResponseTime = currentResponseTime;
////        averageResponseTime += currentResponseTime;
////
////        for(int i = 1; i < totalNumberOfGlobalPredicatesDetected; i++){
////            currentResponseTime = historyResponseTime.elementAt(i);
////            if(maxResponseTime < currentResponseTime)
////                maxResponseTime = currentResponseTime;
////            if(minResponseTime > currentResponseTime)
////                minResponseTime = currentResponseTime;
////            averageResponseTime += currentResponseTime;
////        }
////
////        averageResponseTime = averageResponseTime/ (1.0 * totalNumberOfGlobalPredicatesDetected);
////
////        // for physical time
////        averagePhysicalResponseTime = 0;
////
////        currentPhysicalResponseTime = historyPhysicalResponseTime.elementAt(0);
////        maxPhysicalResponseTime = minPhysicalResponseTime = currentPhysicalResponseTime;
////        averagePhysicalResponseTime += currentPhysicalResponseTime;
////
////        for(int i = 1; i < totalNumberOfGlobalPredicatesDetected; i++){
////            currentPhysicalResponseTime = historyPhysicalResponseTime.elementAt(i);
////            if(maxPhysicalResponseTime < currentPhysicalResponseTime)
////                maxPhysicalResponseTime = currentPhysicalResponseTime;
////            if(minPhysicalResponseTime > currentPhysicalResponseTime)
////                minPhysicalResponseTime = currentPhysicalResponseTime;
////            averagePhysicalResponseTime += currentPhysicalResponseTime;
////        }
////
////        averagePhysicalResponseTime = averagePhysicalResponseTime/ (1.0 * totalNumberOfGlobalPredicatesDetected);
////
////
////    }
//
//}
