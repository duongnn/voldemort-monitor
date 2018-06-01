package predicatedetectionlib.monitor;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.w3c.dom.Element;
import predicatedetectionlib.common.LocalSnapshot;
import predicatedetectionlib.common.XmlUtils;
import predicatedetectionlib.common.predicate.Predicate;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import static predicatedetectionlib.common.CommonUtils.getLocalNonLoopbackAddress;

/**
 * Created by duongnn on 3/27/18.
 */

public class MultiplePredicateMonitor extends Monitor {
    private static long predicateExpirationTime;

    public static int maxMonitorPredicateListSize = 0;
    public static int maxMonitorPredicateStatusSize = 0;

    private static final int INITIAL_DYNAMIC_HASHMAP_SIZE = 1000;
    private static final int INITIAL_NON_DYNAMIC_HASHMAP_SIZE = 50000;

    // Duong debug important
    public static long changePredicateStatusToInactiveSuccess = 0;
    public static long changePredicateStatusToInactiveFail = 0;
    public static long changePredicateStatusToInactiveFailAlreadyInactive = 0;
    public static long changePredicateStatusToInactiveFailCandidate = 0;
    public static long changePredicateStatusToInactiveFailInternalBuffer = 0;
    public static long changePredicateStatusToInactiveFailQueueArray = 0;

    public static ReentrantLock sharedLockForReceiverAndProcessor = new ReentrantLock();

    public static long getPredicateExpirationTime(){
        return predicateExpirationTime;
    }

    public static void main(String[] args){
        // parsing main argument
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> monitorIdOptionSpec = parser.accepts("monitor-id")
                .withRequiredArg().ofType(Integer.class);
        parser.accepts("monitor-input-arg-file")
                .withRequiredArg();

        OptionSet options = parser.parse(args);

        monitorId = options.valueOf(monitorIdOptionSpec);
        String monitorInputFileName = "../../voldemort-related/scripts/" + (String) options.valueOf("monitor-input-arg-file");

        // open monitor input argument file to get the arguments
        Vector<Element> monitorElementList = XmlUtils.getElementListFromFile(monitorInputFileName, "monitor");

        // sanity check
        if(monitorId >= monitorElementList.size()){
            System.out.println(" SinglePredicateMonitor.main() ERROR:");
            System.out.println("   monitorId (" + monitorId + ") >= monitorElementList.size (" + monitorElementList.size() + ")");
            System.exit(1);
        }

        // getting arguments provided via input file
        String[] mainArg = null;
        for(int i = 0; i < monitorElementList.size(); i++){
            Element monitorElement = monitorElementList.elementAt(i);
            String idStr = monitorElement.getElementsByTagName("id").item(0).getTextContent();
            String mainArgStr = monitorElement.getElementsByTagName("mainArg").item(0).getTextContent();
            Element monitorAddrElement = (Element) monitorElement.getElementsByTagName("monitorAddr").item(0);

            if(Integer.valueOf(idStr) == monitorId){
                mainArg = mainArgStr.split("\\s+");
                monitorIp = monitorAddrElement.getElementsByTagName("ip").item(0).getTextContent();
                monitorPortNumber = Integer.valueOf(monitorAddrElement.getElementsByTagName("port").item(0).getTextContent());
                break;
            }
        }

        if(mainArg == null){
            System.out.println(" SinglePredicateMonitor.main ERROR: mainArg is null");
            System.exit(1);
        }

        // parsing arguments provided via input file
        OptionSpec<Integer> numberOfServerOption = parser.accepts("number-of-servers", "number of servers")
                .withRequiredArg()
                .describedAs("number-of-servers")
                .ofType(Integer.class);
        OptionSpec<Long> epsilonOption = parser.accepts("monitor-epsilon", "value of Epsilon, the uncertainty window, in milliseconds")
                .withOptionalArg()
                .describedAs("monitor-epsilon")
                .ofType(Long.class)
                .defaultsTo(300000L); // --epsilon=some_number 300000 is 5 minutes
//        OptionSpec<Integer> monitorPortNumberOption = parser.accepts("monitor-port-number", "which port number the monitor is listening for messages")
//                .withOptionalArg()
//                .describedAs("monitor-port-number")
//                .ofType(Integer.class)
//                .defaultsTo(3308);
        OptionSpec<String> outFileNameOption = parser.accepts("out-file-name", "out-file-name")
                .withOptionalArg()
                .ofType(String.class)
                .defaultsTo("monitor_debug_out");
        OptionSpec<Integer> runIdOptionSpec = parser.accepts("run-id", "sequence number of this run")
                .withRequiredArg()
                .ofType(Integer.class);
        OptionSpec<Long> predicateExpirationTimeSpec = parser.accepts("predicate-expiration-time")
                .withOptionalArg()
                .ofType(Long.class)
                .defaultsTo(1000L);

        options = parser.parse(mainArg);

        numberOfServers = options.valueOf(numberOfServerOption);
        epsilon = options.valueOf(epsilonOption);
        runId = options.valueOf(runIdOptionSpec);
        String outFileName = options.valueOf(outFileNameOption) + "-run" + runId + "-monitor" + monitorId + ".txt";
        predicateExpirationTime = options.valueOf(predicateExpirationTimeSpec);

//        monitorPortNumber = options.valueOf(monitorPortNumberOption);
//        monitorPortNumber = Integer.valueOf(monitorPredicate.getMonitorPortNumber());

        try {
            outFile = new PrintWriter(new FileWriter(outFileName, false));
        }catch(IOException e){
            System.out.println(e.getMessage());
            System.out.println(" could not open file " + outFileName + " to write data");
            System.exit(1);
        }

        //System.out.println(monitorPredicate.toString(0));
        System.out.println("monitor:");
        System.out.println("   id:  " + monitorId);
        System.out.println("   ip:  " + monitorIp);
        System.out.println("   port:" + monitorPortNumber);
        String localNonLoopbackAddr = "not-initialized";
        try{
            localNonLoopbackAddr = getLocalNonLoopbackAddress().toString();
        }catch(Exception e){
            System.out.println("getLocalNonLoopbackAddress() eror: " + e.getMessage());
        }
        System.out.println("   local non-loopback addr: " + localNonLoopbackAddr);

        Monitor.historyResponseTime = new Vector<>();
        Monitor.historyPhysicalResponseTime = new Vector<>();


        // each predicate (identified by unique predicateName string) is mapped to its own data structure
        // the data structure include:
        //    internal buffer     ------.
        //    candidateQueueArray <----'
        //    globalsnapshot: current cut (may not consistent, i.e. candidates may not pairwise concurrent,
        //                                 but the cut is consistent with candidateQueueArray in the sense that
        //                                 globalSnapshot contains all and only those candidates polled out from
        //                                 candidateQueueArray)
        //    predicateList: list of current active predicate
        //    monitorPredicateStatus: record the inactive time (and thus status)
        //            Help us to remove predicate data structure when they are no longer needed
        ConcurrentHashMap<String, Vector<InternalBuffer>> predicateInternalBufferMap = new ConcurrentHashMap<>(INITIAL_DYNAMIC_HASHMAP_SIZE);
        ConcurrentHashMap<String, Vector<LinkedBlockingQueue<LocalSnapshot>>> predicateCandidateQueueArrayMap = new ConcurrentHashMap<>(INITIAL_DYNAMIC_HASHMAP_SIZE);
        ConcurrentHashMap<String, GlobalSnapshot> predicateGlobalSnapshotMap = new ConcurrentHashMap<>(INITIAL_DYNAMIC_HASHMAP_SIZE);
        ConcurrentHashMap<String, Predicate> monitorPredicateList = new ConcurrentHashMap<String, Predicate>(INITIAL_DYNAMIC_HASHMAP_SIZE);
        // monitorPredicateStatus will just be growing
        ConcurrentHashMap<String, Long> monitorPredicateStatus = new ConcurrentHashMap<>(INITIAL_NON_DYNAMIC_HASHMAP_SIZE);

        // initialize and start producer and consumer process
        MultiplePredicateCandidateReceiving producer = new MultiplePredicateCandidateReceiving (
                                                                predicateInternalBufferMap,
                                                                predicateCandidateQueueArrayMap,
                                                                predicateGlobalSnapshotMap,
                                                                monitorPredicateList,
                                                                monitorPredicateStatus);

        MultiplePredicateCandidateProcessing consumer = new MultiplePredicateCandidateProcessing(
                                                                predicateInternalBufferMap,
                                                                predicateCandidateQueueArrayMap,
                                                                predicateGlobalSnapshotMap,
                                                                monitorPredicateList,
                                                                monitorPredicateStatus);

        // start two processes
        stopThreadNow = false;

        new Thread(producer).start();
        new Thread(consumer).start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {

                //Duong debug
                System.out.println(" Receive addShutdownHook signal");
                System.out.println(" Stoping candidate receiving and candidate processing threads");

                stopThreadNow = true;

                updateResponseTimeStatistics();



                String summaryStr = String.format("\n") +
                        String.format("# Total number of global predicates detected =  %,d %n", totalNumberOfGlobalPredicatesDetected) +
                        String.format("#   Average response time =          %,.2f %n", averageResponseTime) +
                        String.format("#   Max response time =              %,d %n", maxResponseTime) +
                        String.format("#   Min reponse time =               %,d %n", minResponseTime) +
                        String.format("# %n") +
                        String.format("#   Average physical response time = %,.2f %n", averagePhysicalResponseTime) +
                        String.format("#   Max physical response time =     %,d %n", maxPhysicalResponseTime) +
                        String.format("#   Min physical reponse time =      %,d %n", minPhysicalResponseTime) +

                        String.format("#   maxMonitorPredicateList.size =   %,d %n", maxMonitorPredicateListSize) +
                        String.format("#   maxMonitorPredicateStatus.size = %,d %n", maxMonitorPredicateStatusSize) +
                        String.format("#   changePredicateStatusToInactiveSuccess = %,d %n", changePredicateStatusToInactiveSuccess) +
                        String.format("#   changePredicateStatusToInactiveFail = %,d %n", changePredicateStatusToInactiveFail) +
                        String.format("#   changePredicateStatusToInactiveFailAlreadyInactive = %,d %n", changePredicateStatusToInactiveFailAlreadyInactive) +
                        String.format("#   changePredicateStatusToInactiveFailCandidate = %,d %n", changePredicateStatusToInactiveFailCandidate) +
                        String.format("#   changePredicateStatusToInactiveFailInternalBuffer = %,d %n", changePredicateStatusToInactiveFailInternalBuffer) +
                        String.format("#   changePredicateStatusToInactiveFailQueueArray = %,d %n", changePredicateStatusToInactiveFailQueueArray) +

                        String.format("#%n");

                System.out.println(summaryStr);

                // write summary message to file
                // this file differs from outFile which is mainly used for debugging
                try {
                    String monitorOutputFileName = "monitorOutputFile-run" + runId +
                            "-monitor" + monitorId + ".txt";
                    PrintWriter printWriter = new PrintWriter(new FileWriter(monitorOutputFileName), true);
                    printWriter.printf("%s", summaryStr);

                    // write out records of response time for figures
                    for(int i = 0; i < totalNumberOfGlobalPredicatesDetected; i++){
                        printWriter.printf(" %10d  %10d  %10d %n",
                                i,
                                Monitor.historyResponseTime.elementAt(i),
                                Monitor.historyPhysicalResponseTime.elementAt(i));
                    }
                    printWriter.close();

                    outFile.close();

                }catch(IOException e){
                    System.out.println(e.getMessage());
                }

            }
        });
    }

    public static void exitDueToError(String errorMessage){
        System.out.println("MultiplePredicateMonitor is going to be stopped due to error");
        System.out.println(errorMessage);

        stopThreadNow = true;

        try {
            // wait for other threads to close
            Thread.sleep(30);
        }catch(Exception e){
            System.out.println("exitDueToError: ERROR: cannot sleep: " + e.getMessage());
        }

        System.exit(1);
    }

    static void markPredicateStatusActive(String predName, ConcurrentHashMap<String, Long> monitorPredicateStatus){

        synchronized (monitorPredicateStatus) {
            monitorPredicateStatus.put(predName, 0L);
        }
    }

    static void markPredicateStatusInactive(String predName, ConcurrentHashMap<String, Long> monitorPredicateStatus){

        synchronized (monitorPredicateStatus) {
            monitorPredicateStatus.put(predName, System.currentTimeMillis());
        }
    }

}
