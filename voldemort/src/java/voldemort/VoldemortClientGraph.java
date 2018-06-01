package voldemort;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.w3c.dom.Element;
import predicatedetectionlib.common.clientgraph.ClientGraphPetersonLock;
import predicatedetectionlib.common.clientgraph.ClientGraphNode;
import predicatedetectionlib.common.XmlUtils;
import predicatedetectionlib.common.clientgraph.ClientGraphNodeType;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.predicatedetection.ClientProxy;
import voldemort.predicatedetection.PredicateDetection;
import voldemort.predicatedetection.debug.DebugPerformance;
import voldemort.predicatedetection.debug.ExperimentVariables;
import voldemort.utils.*;

import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static voldemort.VoldemortClientUtilities.*;


/**
 * Created by duongnn on 8/2/17.
 */
public class VoldemortClientGraph {
    // voldemort client library and some additional functionalities
    VoldemortClientUtilities voldClientUtilities;

    // Duong: variable for the app running mutual exclusion
    private int clientId;


    private String serverIp;

    private String masterWaitProgIp;
    private String masterWaitProgPortNumber;
    private int runId;
    private int numberOfOps;
    private double putProportion;

    private int lineGraphLength;
    private Vector<ClientGraphNode> graphNodeList = new Vector<>();

    public VoldemortClientGraph(VoldemortClientUtilities vcu,
                                             int clientId,
                                             String serverIp,
                                             String masterWaitProgIp,
                                             String masterWaitProgPortNumber,
                                             int runId,
                                             int numberOfOps,
                                             double putProportion,
                                             int lineGraphLength,
                                             Vector<ClientGraphNode> graphNodeList) {

        voldClientUtilities = vcu;

        this.clientId = clientId;

        int slashPos = serverIp.lastIndexOf("/");
        this.serverIp = serverIp.substring(slashPos + 1);
        this.masterWaitProgIp = masterWaitProgIp;
        this.masterWaitProgPortNumber = masterWaitProgPortNumber;
        this.runId = runId;
        this.numberOfOps = numberOfOps;
        this.putProportion = putProportion;
        this.lineGraphLength = lineGraphLength;
        this.graphNodeList = graphNodeList;


        ExperimentVariables.serverIp = new String(serverIp);
        ExperimentVariables.runId = runId;
        ExperimentVariables.numberOfOps = numberOfOps;
        ExperimentVariables.mutableNumberOfOps = numberOfOps;

    }

    // run client graph application
    public void startClientGraphProgram() throws Exception{

        // recording your thread so that the hook can stop you
        ExperimentVariables.mainClientThread = Thread.currentThread();

        ExperimentVariables.operationCount = 0;
        ExperimentVariables.numberOfNodesProcessed = 0;
        ExperimentVariables.insufficientOperationalNodesExceptionCount = 0;

        ExperimentVariables.experimentStartDateTime = DateFormat.getDateTimeInstance().format(System.currentTimeMillis());

        ExperimentVariables.experimentStartNs = System.nanoTime();

        ExperimentVariables.timeForComputation = 0;
        ExperimentVariables.timeForLocks = 0;

        System.out.println("\n\n *************************\n");
        System.out.println(" Starting clientGraph program \n\n");

        // adjusting numberOfOps
        // if numberOfOps = 0, that means monitor will stop clients by SIGINT
        if(numberOfOps == 0) {
            ExperimentVariables.mutableNumberOfOps = Integer.MAX_VALUE;
        }

        // Clear all your flag variables to 0
        clearMyFlags();

        //while (intervalCount < numberOfMeasureIntervals) {
        while(ExperimentVariables.operationCount < ExperimentVariables.mutableNumberOfOps){

            long timeVarNs;

//            // Duong debug
//            System.out.println("operationCount = " + ExperimentVariables.operationCount);

            for (ClientGraphNode currentNode : graphNodeList) {

//                // Duong debug
//                System.out.println("  working on node: " + currentNode.getName());

                // get all the locks associated with this node
//                boolean locksAreObtained = getNodeAllLocks(currentNode);
//                System.out.println("     obtaining locks");

                timeVarNs = System.nanoTime();
                while(!getNodeAllLocks(currentNode));
                ExperimentVariables.timeForLocks += System.nanoTime() - timeVarNs;

//                System.out.println("     locks are obtained");

//                System.out.println(" locksAreObtained = " + locksAreObtained);

                // access critcial section
//                System.out.println("     access critical section");

                timeVarNs = System.nanoTime();
                accessCriticalSection(currentNode);
                ExperimentVariables.timeForComputation += System.nanoTime() - timeVarNs;

                // release locks associated with this node

                timeVarNs = System.nanoTime();
                boolean locksAreReleased = releaseNodeAllLocks(currentNode);

                // Duong debug
                if(!locksAreReleased){
                    System.out.println("startClientGraphProgram: locksAreReleased is false. Something wrong");
                }

                ExperimentVariables.timeForLocks += System.nanoTime() - timeVarNs;

//                System.out.println(" locksAreReleased = " + locksAreReleased);

//                // do computation
//                System.out.println("     do computation");

                timeVarNs = System.nanoTime();
                doComputation(currentNode);
                ExperimentVariables.timeForComputation += System.nanoTime() - timeVarNs;

                ExperimentVariables.numberOfNodesProcessed ++;
            }
        }

        // we only need to flush out information when the program terminated normally
        if(numberOfOps != 0) {

            ExperimentVariables.experimentStopNs = System.nanoTime();

            String experimentStopDateTime = DateFormat.getDateTimeInstance().format(System.currentTimeMillis());

            String clientIpStr = getLocalAddress().toString();
            int slashPos = clientIpStr.lastIndexOf("/");
            clientIpStr = clientIpStr.substring(slashPos + 1);

            // printing summary message
            String summaryString = String.format("%n%% Summary %n") +
                    String.format("%%   Run Id:                     %d %n", runId) +
                    String.format("%%   numberOfOps:                %d %n", numberOfOps) +
                    String.format("%%   Start date time:            %s %n", ExperimentVariables.experimentStartDateTime) +
                    String.format("%%   Stop date time :            %s %n", experimentStopDateTime) +
                    String.format("%%   Predicate detection status: %b %n", PredicateDetection.isActivated()) +
                    String.format("%%   Client Id:                  %d %n", clientId) +
                    String.format("%%   Client IP:                  %s %n", clientIpStr) +
                    String.format("%%   Server IP:                  %s %n", serverIp) +
                    String.format("%%   Total operation count:      %,d %n", ExperimentVariables.operationCount) +
                    String.format("%%   Total time:                 %,d nano seconds %n", (ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) +
                    String.format("%%   Average throughput:         %,.2f ops/sec %n", (1000000000.0 * ExperimentVariables.operationCount) / (ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) +
                    String.format("%%   Average delay:              %,.2f nanosec/op %n", ((1.0) * (ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) / (1.0 * ExperimentVariables.operationCount)) +
                    String.format("%%   Total nodes processed:      %,d %n", ExperimentVariables.numberOfNodesProcessed) +
                    String.format("%%   Average processing speed:   %,.2f nodes/sec %n", (1000000000.0 * ExperimentVariables.numberOfNodesProcessed) / (ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) +
                    String.format("%%   first timestamp:            %,d %n", DebugPerformance.voldemort.getFirstTimestamp()) +
                    String.format("%%   last timestamp:             %,d %n", DebugPerformance.voldemort.getLastTimestamp()) +
                    String.format("%%   time for computation (ns) : %,d %n", ExperimentVariables.timeForComputation) +
                    String.format("%%   time for locks (ns) :       %,d %n", ExperimentVariables.timeForLocks) +
                    String.format("%%   insufficientOperationalNodesExceptionCount :  %,d %n", ExperimentVariables.insufficientOperationalNodesExceptionCount) +
                    String.format("%% %n");
            System.out.println(summaryString);

            // write summary message to file
            try {
                String clientOutputFileName = "../clientOutputFile-run" + runId + "-client" + clientId + ".txt";
                PrintWriter printWriter = new PrintWriter(new FileWriter(clientOutputFileName), true);
                printWriter.printf("%s", summaryString);
                printWriter.close();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }

            DebugPerformance.voldemort.writeDataToFile("../clientThroughput-run" + runId + "-client" + clientId);
            DebugPerformance.application.writeDataToFile("../appThroughput-run" + runId + "-client" + clientId);

            // signal the master that you are done
            sendFinishSignalToMaster(masterWaitProgIp, masterWaitProgPortNumber);
        }
    }

    // clear all the flags/locks associated with nodes belonging to this client
    void clearMyFlags(){
        for (ClientGraphNode currentNode : graphNodeList) {
            releaseNodeAllLocks(currentNode);
        }
    }

    // clear all flags/locks associated with a node
    boolean releaseNodeAllLocks(ClientGraphNode node){
        // internal nodes have no lock
        if(node.getType() == ClientGraphNodeType.INTERNAL)
            return true;

        return releaseNodeAllLocksPeterson(node);
    }

    boolean getNodeAllLocks(ClientGraphNode node){
        // internal nodes have no lock
        if(node.getType() == ClientGraphNodeType.INTERNAL)
            return true;

        return getNodeAllLocksPeterson(node, 0);
    }

    /**
     * access critical section at a node
     * @param node
     * @return number of operations performed on this node
     */
    int accessCriticalSection(ClientGraphNode node){
        int numberOfOps = 0;

        System.out.println("\n  accessCriticalSection:");

        // we assume when accessing critical section, client will read values of its
        // neighbor. The number of neighbor should be around 10
        for(int i = 0; i < 10; i++){
            int randId = ThreadLocalRandom.current().nextInt(0, 1000+1);
            int randVal = ThreadLocalRandom.current().nextInt(0, 1000+1);

            if(randId % (int)(1/putProportion) == 0) {
                // issue a put/write
                voldClientUtilities.setVariable("cs" + randId, String.valueOf(randVal));
            }else{
                // issue a get/read
                voldClientUtilities.getVariable("cs" + randId);
            }

//            if((i+1)%1000 == 0)
//                System.out.println("     *");

            numberOfOps ++;
        }

        return numberOfOps;
    }

    /**
     * do a computation on a node
     * @param node
     * @return number of operations performed
     */
    int doComputation(ClientGraphNode node){
        int numberOfOps = 0;

        System.out.println("\n  doComputation:");

        // the amount of times for doing computation is often 10 to 20 times the amount of
        // critical section (according to some reading related to MapReduce)
        // int numberOfRandomOps = ThreadLocalRandom.current().nextInt(10, 20);

        // however, when delay is as high as >= 50 ms, then the computation should be
        // done in parallel with communication, thus we make the mount of time for computation
        // roughly the same as communication (accessCriticalSection)
        int numberOfRandomOps = 2;

        for(int i = 0; i < numberOfRandomOps * 10; i++) {
            int randId = ThreadLocalRandom.current().nextInt(0, 1000 + 1);
            int randVal = ThreadLocalRandom.current().nextInt(0, 1000 + 1);

            if (randId % (int)(1/putProportion) == 0){
                voldClientUtilities.setVariable("n" + randId, String.valueOf(randVal));
            }else{
                voldClientUtilities.getVariable("n" + randId);
            }

//            if((i+1)%1000 == 0)
//                System.out.println("          --");

            numberOfOps ++;
        }

        return numberOfOps;
    }

    void justSleep(){
        try{
            for(int i = 0; i < 10; i++) {
                System.out.println("     *");
                Thread.sleep(1000L);
            }
        }catch(InterruptedException e){
            System.out.println(" get InterruptedException " + e.getMessage());
            e.printStackTrace();
        }

    }



    /**
     * Get the all individual locks associated with a graph node
     * So that we can safely access critical section on the node
     * @param node: node whose locks to be obtained
     * @param timeOutNs: return after time out. timeOutNs <= 0 means no time out
     * @return true if all locks of this nodes are obtained before timeout
     *         false if not all locks are obtained before timeout
     *               or some lock cannot be obtained
     */

    boolean getNodeAllLocksPeterson(ClientGraphNode node, long timeOutNs){
        long startNs = System.nanoTime();

        while(true){
            for(Map.Entry<String, ClientGraphPetersonLock> entry : node.getLockHashMap().entrySet()){
                ClientGraphPetersonLock lock = entry.getValue();

                // request to obtain this lock
                long remainTimeNs;
                if(timeOutNs > 0){
                    remainTimeNs = timeOutNs - (System.nanoTime() - startNs);
                    if(remainTimeNs <= 0)
                        return false;
                }else{
                    remainTimeNs = 0;
                }
                if(!voldClientUtilities.getIndividualLockPeterson(
                        lock.getNodeFlagVar(),
                        lock.getNeighborFlagVar(),
                        lock.getTurnVar(),
                        node.getName(),
                        remainTimeNs)){
                    return false;
                }
            }

            // check if timeout?
            if(timeOutNs > 0){
                if(System.nanoTime() - startNs >= timeOutNs)
                    return false;
            }else{
                return true;
            }
        }
    }


    /**
     * Release the all individual locks associated with a graph node
     * @param node: node whose locks to be obtained
     * @return true if all locks of this nodes are obtained
     *         false if not all locks are obtained before timeout
     */

    boolean releaseNodeAllLocksPeterson(ClientGraphNode node){
        for(Map.Entry<String, ClientGraphPetersonLock> entry : node.getLockHashMap().entrySet()){
            ClientGraphPetersonLock lock = entry.getValue();
            if(!voldClientUtilities.releaseIndividualLockPeterson(lock.getNodeFlagVar()))
                return false;
        }

        return true;
    }

    //////////////////////////////////////////////////////////////////////////////


    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        // parsing main function arguments
        OptionParser mainParser = new OptionParser();

        OptionSpec<Integer> clientIdOptionSpec = mainParser.accepts("client-id")
                .withRequiredArg()
                .ofType(Integer.class); //--client-id=0/1/2/3
        mainParser.accepts("client-input-arg-file")
                .withRequiredArg();
        mainParser.accepts("voldemort-shell", "Suffix of shell script; used to format help output."
                + " Examples of script suffixes: sh, bat, app")
                .withRequiredArg()
                .describedAs("script_suffix");

        OptionSet mainOption = mainParser.parse(args);


        String clientInputArgFileName = (String) mainOption.valueOf("client-input-arg-file");
        final int clientId = mainOption.valueOf(clientIdOptionSpec);

        // reading list client element from xml file
        Vector<Element> clientElementList = XmlUtils.getElementListFromFile(clientInputArgFileName, "client");
        int numberOfClients = clientElementList.size();
        if(clientId >= numberOfClients){
            System.out.println(" VoldemortClientGraph.main() ERROR");
            System.out.println("  clientId (" + clientId + ") >= numberOfClients (" + numberOfClients + ")");
            System.exit(-1);
        }

        // extracting arguments for this client from xml element
        String[] mainArg = null;
        Element graphNodesElement = null;
        for(int i = 0; i < numberOfClients; i++){
            Element clientElement = clientElementList.elementAt(i);
            String idStr = clientElement.getElementsByTagName("id").item(0).getTextContent();
            String mainArgStr = clientElement.getElementsByTagName("mainArg").item(0).getTextContent();
            graphNodesElement = (Element) clientElement.getElementsByTagName("graphNodes").item(0);

            if(clientId == Integer.valueOf(idStr)){
                mainArg = mainArgStr.split("\\s+");
                break;
            }
        }

        if(mainArg == null){
            System.out.println(" VoldemortClientGraph.main() ERROR: mainArg is null");
            System.exit(-1);
        }

        // processing input arguments read from file
        OptionParser parser = new OptionParser();
        VoldemortClientUtilities.parserAcceptanceConfig(parser);

        final OptionSet options = parser.parse(mainArg);

        List<String> nonOptions = (List<String>) options.nonOptionArguments();
        if(nonOptions.size() < 2 || nonOptions.size() > 3 || options.has("help")) {
            if (mainOption.has("voldemort-shell")) {
                System.err.println("Usage: voldemort-shell."
                        + mainOption.valueOf("voldemort-shell")
                        + " store_name bootstrap_url [command_file] [options]");
            } else {
                System.err.println("Usage: java VoldemortClientGraph --client-id=0/1/2 --client-input-arg-file=file.xml");
            }
            mainParser.printHelpOn(System.err);
            System.exit(-1);
        }

        String storeName = nonOptions.get(0);
        String bootstrapUrl = nonOptions.get(1);

        String configFile = (String) options.valueOf("config-file");
        ClientConfig clientConfig = null;
        BufferedReader inputReader = null;
        boolean fileInput = false;
        DebugPerformance.voldemort.setIncrementStep(options.valueOf(incrementStepOptionSpec));
        DebugPerformance.application.setIncrementStep(options.valueOf(incrementStepOptionSpec));
        final int lineGraphLengthPerClient = options.valueOf(lineGraphLengthPerClientSpec);
        final int lineGraphLength = lineGraphLengthPerClient*numberOfClients;


        ClientProxy.setProxyIsEnabled(options.valueOf(proxyEnableOptionSpec));
        if(ClientProxy.isProxyIsEnabled()){
            String proxyStr = bootstrapUrl;

            int slashPos = proxyStr.lastIndexOf("/");
            proxyStr = proxyStr.substring(slashPos + 1);
            String[] proxyStrSplit = proxyStr.split(":");

            ClientProxy.setProxyIpAddress(proxyStrSplit[0]);
            ClientProxy.setProxyPortNumber(proxyStrSplit[1]);
            ClientProxy.setProxyBasePortNumber(options.valueOf(proxyBasePortNumberSpec));
        }


        PredicateDetection.setActivated(options.valueOf(predDetectionOption));

        // Duong debug
        if(PredicateDetection.isActivated())
            System.out.println(" Predicate Detection is enabled ");
        else
            System.out.println(" Predicate Detection is NOT enabled ");


        try {
            if(nonOptions.size() == 3) {
                inputReader = new BufferedReader(new FileReader(nonOptions.get(2)));
                fileInput = true;
            } else {
                inputReader = new BufferedReader(new InputStreamReader(System.in));
            }
        } catch(IOException e) {
            Utils.croak("Failure to open input stream: " + e.getMessage());
        }

        if (configFile != null) {
            clientConfig = new ClientConfig(new File(configFile));
        } else {
            clientConfig = new ClientConfig();
        }

        clientConfig.setBootstrapUrls(bootstrapUrl)
                .setEnableLazy(false)
                .setRequestFormatType(RequestFormatType.VOLDEMORT_V3);

        if(options.has("client-zone-id")) {
            clientConfig.setClientZoneId((Integer) options.valueOf("client-zone-id"));
        }

        // read the list of nodes assigned to this client
        Vector<ClientGraphNode> graphNodeList = new Vector<>();
        Vector<Element> nodeElementList = XmlUtils.getElementListFromNode(graphNodesElement, "node");
        for(int i = 0; i < nodeElementList.size(); i++){
            graphNodeList.addElement(XmlUtils.elementToClientGraphNode(nodeElementList.elementAt(i)));
        }

        // since the xml does not have information about lock, we have to compute it
        // we should not integerate this step with the step above (reading nodes from xml
        // since this step requires full information about all nodes belonging to this client
        for(ClientGraphNode node : graphNodeList){
            // init the lockHashMap for the node
            HashMap<String, ClientGraphPetersonLock> lockHashMap = new HashMap<>();

            // for each of the neighbors (left and right) that belongs to another client, we add a lock
            if(node.getType() == ClientGraphNodeType.BORDER){
                String idStr = node.getName();
                int id = Integer.valueOf(idStr);
                int leftId = id - 1;
                int rightId = id + 1;

                // check if leftId and rightId is within my assignment
                boolean leftIdIsMine = false;
                boolean rightIdIsMine = false;
                for(ClientGraphNode cgNode : graphNodeList){
                    int cgNodeId = Integer.valueOf(cgNode.getName());
                    if(cgNodeId == leftId){
                        leftIdIsMine = true;
                    }
                    if(cgNodeId == rightId){
                        rightIdIsMine = true;
                    }
                }

                if(!leftIdIsMine && leftId >= 0){
                    ClientGraphPetersonLock lock = new ClientGraphPetersonLock(
                                                            node.getName(),
                                                            String.valueOf(leftId),
                                                            "flag" + leftId + id + "_" + id,
                                                            "flag" + leftId + id + "_" + leftId,
                                                            "turn" + leftId + id);
                    lockHashMap.put(String.valueOf(leftId), lock);
                }


                if(!rightIdIsMine && (rightId <= lineGraphLength - 1)){
                    ClientGraphPetersonLock lock = new ClientGraphPetersonLock(
                                                            node.getName(),
                                                            String.valueOf(leftId),
                                                            "flag" + id + rightId + "_" + id,
                                                            "flag" + id + rightId + "_" + rightId,
                                                            "turn" + id + rightId);
                    lockHashMap.put(String.valueOf(rightId), lock);
                }
            }else{
                // internal node does not need lock
            }

            node.setLockHashMap(lockHashMap);
        }

        // configuring the voldemort client library
        VoldemortClientUtilities vcu = new VoldemortClientUtilities(clientConfig,
                storeName,
                inputReader,
                System.out,
                System.err);


        VoldemortClientGraph voldClientGraph = new VoldemortClientGraph(vcu,
                clientId,
                bootstrapUrl,
                (String) options.valueOf("master-ip-addr"),
                (String) options.valueOf("master-wait-prog-waiting-port"),
                options.valueOf(runIdOptionSpec),
                options.valueOf(opCountSpec),
                options.valueOf(putProportionSpec),
                lineGraphLength,
                graphNodeList);


        // add a shutdown hook in case client is stopped by SIGINT
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {

                ExperimentVariables.mutableNumberOfOps = 0;

                // saving data
                System.out.println("\n\n ******* Client received SIGINT *******\n\n");

                ExperimentVariables.experimentStopNs = System.nanoTime();

                String experimentStopDateTime = DateFormat.getDateTimeInstance().format(System.currentTimeMillis());

                String clientIpStr = "";
                try {
                    clientIpStr = getLocalAddress().toString();

                    int slashPos = clientIpStr.lastIndexOf("/");
                    clientIpStr = clientIpStr.substring(slashPos + 1);
                }catch(SocketException e){
                    clientIpStr=new String("Error: can't get clientIp: " + e.getMessage());
                }


                // printing summary message
                String summaryString = String.format("%n%% Summary %n") +
                        String.format("%%   Run Id:                     %d %n", ExperimentVariables.runId) +
                        String.format("%%   numberOfOps:                %d %n", ExperimentVariables.numberOfOps) +
                        String.format("%%   Start date time:            %s %n", ExperimentVariables.experimentStartDateTime) +
                        String.format("%%   Stop date time :            %s %n", experimentStopDateTime) +
                        String.format("%%   Predicate detection status: %b %n", PredicateDetection.isActivated()) +
                        String.format("%%   Client program:             %s %n", clientId) +
                        String.format("%%   Client IP:                  %s %n", clientIpStr) +
                        String.format("%%   Server IP:                  %s %n", ExperimentVariables.serverIp) +
                        String.format("%%   Total operation count:      %,d %n", ExperimentVariables.operationCount) +
                        String.format("%%   Total time:                 %,d nano seconds %n", (ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) +
                        String.format("%%   Average throughput:         %,.2f ops/sec %n", (Time.NS_PER_SECOND*1.0*ExperimentVariables.operationCount)/(ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) +
                        String.format("%%   Average delay:              %,.2f nanosec/op %n", ((1.0)*(ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs))/(1.0*ExperimentVariables.operationCount)) +
                        String.format("%%   Total nodes processed:      %,d %n", ExperimentVariables.numberOfNodesProcessed) +
                        String.format("%%   Average processing speed:   %,.2f nodes/sec %n", (1000000000.0 * ExperimentVariables.numberOfNodesProcessed) / (ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) +
                        String.format("%%   first timestamp:            %,d %n", DebugPerformance.voldemort.getFirstTimestamp()) +
                        String.format("%%   last timestamp:             %,d %n", DebugPerformance.voldemort.getLastTimestamp()) +
                        String.format("%%   time for computation (ns) : %,d %n", ExperimentVariables.timeForComputation) +
                        String.format("%%   time for locks (ns) :       %,d %n", ExperimentVariables.timeForLocks) +
                        String.format("%%   insufficientOperationalNodesExceptionCount :  %,d %n", ExperimentVariables.insufficientOperationalNodesExceptionCount) +
                        String.format("%% %n");
                System.out.println(summaryString);

                // write summary message to file
                try {
                    String clientOutputFileName = "../clientOutputFile-run"+ExperimentVariables.runId+"-client"+clientId+".txt";
                    PrintWriter printWriter = new PrintWriter(new FileWriter(clientOutputFileName), true);
                    printWriter.printf("%s", summaryString);
                    printWriter.close();
                }catch(IOException e){
                    System.out.println(e.getMessage());
                }

                DebugPerformance.voldemort.writeDataToFile("../clientThroughput-run" + ExperimentVariables.runId +
                        "-client" + clientId);
                DebugPerformance.application.writeDataToFile("../appThroughput-run" + ExperimentVariables.runId +
                        "-client" + clientId);
            }
        });


        voldClientGraph.startClientGraphProgram();

    }



}
