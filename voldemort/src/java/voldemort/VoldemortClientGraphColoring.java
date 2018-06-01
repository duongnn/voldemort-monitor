package voldemort;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.w3c.dom.Element;
import predicatedetectionlib.common.XmlUtils;
import predicatedetectionlib.common.clientgraph.ClientGraphPetersonLock;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.predicatedetection.ClientProxy;
import voldemort.predicatedetection.PredicateDetection;
import voldemort.predicatedetection.debug.DebugPerformance;
import voldemort.predicatedetection.debug.ExperimentVariables;
import voldemort.utils.Time;
import voldemort.utils.Utils;

import java.io.*;
import java.net.SocketException;
import java.text.DateFormat;
import java.util.*;

import static voldemort.VoldemortClientUtilities.*;
import static voldemort.VoldemortClientUtilities.getLocalAddress;
import static voldemort.predicatedetection.debug.ExperimentVariables.*;

/**
 * Created by duongnn on 3/11/18.
 */
public class VoldemortClientGraphColoring {
    // voldemort client library and some additional functionalities
    VoldemortClientUtilities voldClientUtilities;

    public static final String GRAPH_NODE_STATUS_COLORED = "1";
    public static final String GRAPH_NODE_STATUS_UNCOLORED = "0";

    // Duong: variable for the app running mutual exclusion
    private int clientId;
    private int numberOfClients;

    private String serverIp;

    private String masterWaitProgIp;
    private String masterWaitProgPortNumber;

    private int runId;
    private int numberOfOps;

    private static String graphType;
    private static int numberOfGraphNodes;
    private static int numberOfEdges;
    private static int maxDegree;
    private static int maxNumberOfColors;

    // these are calculated outside the constructor
    private int taskSize;
    private int totalNumberOfTasks;

    private boolean preprocessHighDegreeNodes;
    private String highDegreeNodeColorFileName;
    private HashMap<String, String> highDegreeNodeColorMap;

    private boolean readNeighborColorByGetall;

    public VoldemortClientGraphColoring(VoldemortClientUtilities vcu,
                                int clientId,
                                int numberOfClients,
                                String serverIp,
                                String masterWaitProgIp,
                                String masterWaitProgPortNumber,
                                int runId,
                                int numberOfOps,
                                int taskSize,
                                boolean preprocessHighDegreeNodes,
                                String highDegreeNodeColorFileName,
                                boolean readNeighborColorByGetall) {

        voldClientUtilities = vcu;

        this.clientId = clientId;
        this.numberOfClients = numberOfClients;

        int slashPos = serverIp.lastIndexOf("/");
        this.serverIp = serverIp.substring(slashPos + 1);
        this.masterWaitProgIp = masterWaitProgIp;
        this.masterWaitProgPortNumber = masterWaitProgPortNumber;
        this.runId = runId;
        this.numberOfOps = numberOfOps;

        this.taskSize = taskSize;

        this.preprocessHighDegreeNodes = preprocessHighDegreeNodes;
        if(preprocessHighDegreeNodes) {
            // reading color of high degree nodes
            long startMs = System.currentTimeMillis();
            this.highDegreeNodeColorFileName = highDegreeNodeColorFileName;
            highDegreeNodeColorMap = new HashMap<>();
            File highDegreeNodeColorFile = new File(highDegreeNodeColorFileName);
            try {
                BufferedReader reader = new BufferedReader(new FileReader(highDegreeNodeColorFile));
                String currentLine = reader.readLine();

                while (currentLine != null) {
                    int semicolonPos = currentLine.indexOf(':');
                    String nodeIdStr = currentLine.substring(0, semicolonPos);
                    String nodeColorStr = currentLine.substring(semicolonPos + 1);

                    highDegreeNodeColorMap.put(nodeIdStr, nodeColorStr);
                    currentLine = reader.readLine();
                }
            } catch (IOException ioe) {
                System.out.println(" Could not open or read file " + highDegreeNodeColorFileName);
                System.out.println(ioe.getMessage());
            }

            System.out.println("\n  finish reading high degree node color file in " +
                    (System.currentTimeMillis() - startMs) + " ms");
        }

        this.readNeighborColorByGetall = readNeighborColorByGetall;

        ExperimentVariables.serverIp = new String(serverIp);
        ExperimentVariables.runId = runId;
        ExperimentVariables.numberOfOps = numberOfOps;
        ExperimentVariables.mutableNumberOfOps = numberOfOps;

    }

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


        // configuring the voldemort client library
        VoldemortClientUtilities vcu = new VoldemortClientUtilities(clientConfig,
                storeName,
                inputReader,
                System.out,
                System.err);


        VoldemortClientGraphColoring voldClientGraph = new VoldemortClientGraphColoring(vcu,
                clientId,
                numberOfClients,
                bootstrapUrl,
                (String) options.valueOf("master-ip-addr"),
                (String) options.valueOf("master-wait-prog-waiting-port"),
                options.valueOf(runIdOptionSpec),
                options.valueOf(opCountSpec),
                options.valueOf(taskSizeSpec),
                options.valueOf(preprocessHighDegreNodesSpec),
                (String) options.valueOf("high-degree-node-color-filename"),
                options.valueOf(readNeighborColorByGetallSpec));

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
                        String.format("%%   numberOfClients:            %d %n", numberOfClients) +
                        String.format("%%   numberOfGraphNodes:         %d %n", numberOfGraphNodes) +
                        String.format("%%   taskSize:                   %s %n", options.valueOf(taskSizeSpec)) +
                        String.format("%%   number of tasks:            %d %n", myTaskAssignment[1] - myTaskAssignment[0]) +
                        String.format("%%   number of tasks completed:  %d %n", currentTaskId - myTaskAssignment[0]) +
                        String.format("%%   number of border nodes:     %d %n", numberOfBorderNodes) +
                        String.format("%%   number of locks:            %d %n", numberOfLocks) +
                        String.format("%%   Total time:                 %,d nano seconds %n", (ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) +
                        String.format("%%   first timestamp:            %,d %n", DebugPerformance.voldemort.getFirstTimestamp()) +
                        String.format("%%   last timestamp:             %,d %n", DebugPerformance.voldemort.getLastTimestamp()) +
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


        voldClientGraph.startClientGraphColoringProgram();

    }

    public void startClientGraphColoringProgram() throws Exception {
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

        // obtain number of graph nodes
        graphType = voldClientUtilities.getVariable("g_type");
        numberOfGraphNodes = Integer.parseInt(voldClientUtilities.getVariable("g_numberOfNodes"));
        numberOfEdges = Integer.parseInt(voldClientUtilities.getVariable("g_numberOfEdges"));
        maxDegree = Integer.parseInt(voldClientUtilities.getVariable("g_maxDegree"));
        maxNumberOfColors = Integer.parseInt(voldClientUtilities.getVariable("g_maxNumberOfColors"));

        // Duong debug
        System.out.println("graphType = " + graphType);
        System.out.println("numberOfGraphNodes = " + numberOfGraphNodes);
        System.out.println("numberOfEdges = " + numberOfEdges);
        System.out.println("maxDegree = " + maxDegree);
        System.out.println("maxNumberOfColors = " + maxNumberOfColors);

        // construct task list
        totalNumberOfTasks = (int) Math.ceil(numberOfGraphNodes*1.0/taskSize);

        // myTaskAssignment[0] is id of first task
        // myTaskAssignment[1] is id of last task
        getTaskAssignment(totalNumberOfTasks, numberOfClients, clientId, myTaskAssignment);
        doMyTaskAssignment(myTaskAssignment);

        System.out.println("\n Client " + clientId + " finishes all the tasks in " +
                (System.nanoTime() - ExperimentVariables.experimentStartNs)/Time.NS_PER_MS + " milliseconds");


        // we only need to flush out information when the program terminated normally
        if(numberOfOps != 0) {

            ExperimentVariables.experimentStopNs = System.nanoTime();

            String experimentStopDateTime = DateFormat.getDateTimeInstance().format(System.currentTimeMillis());

            String clientIpStr = getLocalAddress().toString();
            int slashPos = clientIpStr.lastIndexOf("/");
            clientIpStr = clientIpStr.substring(slashPos + 1);

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
                    String.format("%%   numberOfClients:            %d %n", numberOfClients) +
                    String.format("%%   numberOfGraphNodes:         %d %n", numberOfGraphNodes) +
                    String.format("%%   taskSize:                   %s %n", taskSize) +
                    String.format("%%   number of tasks:            %d %n", myTaskAssignment[1] - myTaskAssignment[0]) +
                    String.format("%%   number of tasks completed:  %d %n", currentTaskId - myTaskAssignment[0]) +
                    String.format("%%   number of border nodes:     %d %n", numberOfBorderNodes) +
                    String.format("%%   number of locks:            %d %n", numberOfLocks) +
                    String.format("%%   Total time:                 %,d nano seconds %n", (ExperimentVariables.experimentStopNs - ExperimentVariables.experimentStartNs)) +
                    String.format("%%   first timestamp:            %,d %n", DebugPerformance.voldemort.getFirstTimestamp()) +
                    String.format("%%   last timestamp:             %,d %n", DebugPerformance.voldemort.getLastTimestamp()) +
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

    // do all the tasks assigned to this client
    void doMyTaskAssignment(int[] myTaskAssignment){
        int firstTaskId = myTaskAssignment[0];
        int lastTaskId = myTaskAssignment[1];

        int firstNodeIdOfFirstTask;
        int firstNodeIdOfLastTask, lastNodeIdOfLastTask;

        firstNodeIdOfFirstTask = firstTaskId * taskSize;
        firstNodeIdOfLastTask = lastTaskId * taskSize;

        if(lastTaskId == totalNumberOfTasks - 1){
            // last task
            lastNodeIdOfLastTask = numberOfGraphNodes - 1;
        }else{
            lastNodeIdOfLastTask = firstNodeIdOfLastTask + taskSize - 1;
        }


        for(currentTaskId = firstTaskId; currentTaskId <= lastTaskId; currentTaskId ++){

            // Duong debug
            System.out.println(" processing task " + currentTaskId + " in [" + firstTaskId + ", " + lastTaskId + "]");

            doTask(currentTaskId, firstNodeIdOfFirstTask, lastNodeIdOfLastTask);
        }
    }

    // do a particular task
    void doTask(int taskId, int firstNodeIdOfFirstTask, int lastNodeIdOfLastTask){
        long taskStartMs = System.currentTimeMillis();

        // obtain list of nodes corresponding to this task
        int firstNodeId, lastNodeId;

        firstNodeId = taskId * taskSize;
        if(taskId == totalNumberOfTasks - 1){
            // last task
            lastNodeId = numberOfGraphNodes - 1;
        }else{
            lastNodeId = firstNodeId + taskSize - 1;
        }

        for(int currentNodeId = firstNodeId; currentNodeId <= lastNodeId; currentNodeId ++){
            long colorNodeStartMs = System.currentTimeMillis();

            colorNode(currentNodeId, firstNodeId, lastNodeId, firstNodeIdOfFirstTask, lastNodeIdOfLastTask);

            System.out.println("    timelog: color node " + currentNodeId +
                    " in " + (System.currentTimeMillis() - colorNodeStartMs) + " ms");
        }

        System.out.println("    timelog: finish task " + taskId +
                " in " + (System.currentTimeMillis() - taskStartMs) + " ms");

    }



    /**
     * Color a particular node
     * @param nodeId
     * @param firstNodeIdInSameTask help to determine if nodeId is internal or external node
     * @param lastNodeIdInSameTask help to determine if nodeId is internal or external node
     * @param firstNodeIdOfFirstTask help to determine if nodeId is internal or external more efficiently
     *                               in the sense that some node can be external for a task, but still
     *                               internal for a client (a group of tasks)
     * @param lastNodeIdOfLastTask help to determine if nodeId is internal or external more efficiently
     */
    void colorNode(int nodeId, int firstNodeIdInSameTask, int lastNodeIdInSameTask,
                   int firstNodeIdOfFirstTask, int lastNodeIdOfLastTask){

//        // Duong debug
//        System.out.println(" Going to color node " + nodeId);

        String nodeIdStr = Integer.toString(nodeId);

        if(preprocessHighDegreeNodes) {
            // check if node is in high degree node list which has already been colored
            if (highDegreeNodeColorMap.containsKey(nodeIdStr)) {

                // Duong debug
                System.out.println("  " + nodeIdStr + " is high degree node");

                return;
            }
        }

        // 1. Read from server color of node and list of node neighbors
        int originalColor = Integer.parseInt(voldClientUtilities.getVariable("v_" + nodeIdStr + "_col"));
        int newColor;
        String[] nodeNbrStrList = voldClientUtilities.getVariable("v_" + nodeIdStr + "_nbr").split("\\s+");
        int numberOfNbrs = nodeNbrStrList.length;

        // in order to avoid deadlock, we have to make sure the locks are sorted in ascending order
        int[] nodeNbrListSorted = new int[nodeNbrStrList.length];
        for(int i = 0; i < numberOfNbrs; i ++){
            nodeNbrListSorted[i] = Integer.valueOf(nodeNbrStrList[i]);
        }
        Arrays.sort(nodeNbrListSorted);

        // 2. Obtain all locks associated with edges incident to this node (if that edge is external)
        // 3. And read colors of all neighbors
        int[] nbrColorList = new int[numberOfNbrs];
        Vector<ClientGraphPetersonLock> lockList = new Vector<>();

        readColorOfNeighborNodes(nodeId, firstNodeIdOfFirstTask, lastNodeIdOfLastTask,nodeNbrListSorted,
                nbrColorList, lockList);

        // 4. Pick best possible color
            // 4.1. If graphType == "powerlaw_cluster_graph"
                // p = maxNumOfColors/2
                // if your degree > p
                    // choose minimal possible colors from p+1 to 2p
                // else
                    // choose minimal possible colors from 0 to p
            // 4.2. else
                // choose minimal possible colors from 0 to maxNumOfColors

        if(graphType.equals("powerlaw_cluster_graph")){
            int p = maxNumberOfColors/2;
            if(numberOfNbrs > p){
                // use color between p+1 maxNumberOfColors that is not used by neighbors
                newColor = getMinimalValueNotInArray(nbrColorList, p+1, maxNumberOfColors);
            }else{
                // use color between 0 and p that is not used by neighbors
                newColor = getMinimalValueNotInArray(nbrColorList, 0, p);
            }
        }else{
            newColor = getMinimalValueNotInArray(nbrColorList, 0, maxNumberOfColors);
        }

        // color node and mark status as COLORED
        voldClientUtilities.setVariable("v_" + nodeId + "_col", Integer.toString(newColor));
        voldClientUtilities.setVariable("v_" + nodeId + "_ste", GRAPH_NODE_STATUS_COLORED);

        // 5. Release all locks
        for(ClientGraphPetersonLock l : lockList){
            //voldClientUtilities.releaseIndividualLockPeterson(l);
            voldClientUtilities.releaseIndividualLockPetersonEnhanced(l);
        }

    }

    /**
     * obtain necessary locks
     * and read color of neighbors
     * @param nodeId                    in
     * @param firstNodeIdOfFirstTask    in
     * @param lastNodeIdOfLastTask      in
     * @param nodeNbrListSorted         in
     * @param nbrColorList              out
     * @param lockList                  out
     */
    private void readColorOfNeighborNodes(
            int nodeId,
            int firstNodeIdOfFirstTask,
            int lastNodeIdOfLastTask,
            int[] nodeNbrListSorted,
            int[] nbrColorList,
            Vector<ClientGraphPetersonLock> lockList){

        int numberOfNbrs = nodeNbrListSorted.length;
        String nodeIdStr = Integer.toString(nodeId);
        String getAllArgStr = "";

        for(int nbr = 0; nbr < numberOfNbrs; nbr++){
            int nbrId = nodeNbrListSorted[nbr];
            String nbrStr = String.valueOf(nbrId);

            if(preprocessHighDegreeNodes) {
                // if neighbor is a high degree node, we can read from the local data
                if (highDegreeNodeColorMap.containsKey(nbrStr)) {
                    int nbrColor = Integer.parseInt(highDegreeNodeColorMap.get(nbrStr).trim());

                    nbrColorList[nbr] = nbrColor;
                    continue;
                }
            }

            // lock the edge if both condition is met:
            //   + the neighbor is external, i.e. not in the same task. If it is in the same task
            //     then no other client will work on this neighbor
            //   + the neighbor has not been colored yet. If it has been colored, then no other
            //     client is working on this neighbor
            //   + it is assigned to other client
            if((nbrId < firstNodeIdOfFirstTask) || (nbrId > lastNodeIdOfLastTask)){
                String nbrStatus = voldClientUtilities.getVariable("v_" + nbrStr + "_ste");
                if(nbrStatus == null || nbrStatus.equals(GRAPH_NODE_STATUS_UNCOLORED)) {
                    // this neighbor is not yet colored. We need lock
                    String bothId;
                    if (nbrId < nodeId) {
                        bothId = nbrStr + "_" + nodeIdStr;
                    } else {
                        bothId = nodeIdStr + "_" + nbrStr;
                    }

                    // this node has external edge, need lock for this edge
                    ClientGraphPetersonLock l = new ClientGraphPetersonLock(nodeIdStr,
                            nbrStr,
                            "flag" + bothId + "_" + nodeIdStr,
                            "flag" + bothId + "_" + nbrStr,
                            "turn" + bothId);

                    lockList.addElement(l);

                    // obtain lock
                    //voldClientUtilities.getIndividualLockPeterson(l, 0);
                    while(!voldClientUtilities.getIndividualLockPetersonEnhanced(l, 0)){
                        System.out.println("getIndividualLockPetersonEnhanced() return false");
                    }
                }
            }

            // read color of neighbor if you prefer to do so right now
            if(!readNeighborColorByGetall) {
                nbrColorList[nbr] = Integer.parseInt(voldClientUtilities.getVariable("v_" + nbrStr + "_col"));
            }else{
                getAllArgStr = getAllArgStr + " " + "\"" + "v_" + nbrStr + "_col" + "\"";
            }
        }

        // read all neighbors color in one getall request to reduce number of round trips
        if(readNeighborColorByGetall){
            HashMap<String, String> getAllResults = voldClientUtilities.getAllVariables(getAllArgStr);

            for(int nbr = 0; nbr < numberOfNbrs; nbr++) {
                int nbrId = nodeNbrListSorted[nbr];
                String nbrStr = String.valueOf(nbrId);

                String nbrColName = "v_" + nbrStr + "_col";
                String nbrColValue = getAllResults.get(nbrColName);

                if(nbrColValue != null){
                    nbrColorList[nbr] = Integer.parseInt(nbrColValue);
                }
            }
        }

    }

    /**
     * pretend to color a node
     * The purpose is to get number of border nodes and number of locks
     * @param nodeId
     * @param firstNodeIdInSameTask
     * @param lastNodeIdInSameTask
     */
    void pseudoColorNode(int nodeId, int firstNodeIdInSameTask, int lastNodeIdInSameTask){
        // 1. Read from server color of node and list of node neighbors
        String nodeIdStr = Integer.toString(nodeId);
        int originalColor = Integer.parseInt(voldClientUtilities.getVariable("v_" + nodeIdStr + "_col"));
        int newColor;
        String[] nodeNbrStrList = voldClientUtilities.getVariable("v_" + nodeIdStr + "_nbr").split("\\s+");
        int numberOfNbrs = nodeNbrStrList.length;
        int[] nbrColorList = new int[numberOfNbrs];

        boolean iAmIsBorder = false;

        // 2. Check if any neighbr is external
        Vector<ClientGraphPetersonLock> lockList = new Vector<>();
        for(int nbr = 0; nbr < numberOfNbrs; nbr++){
            String nbrStr = nodeNbrStrList[nbr];
            int nbrId = Integer.parseInt(nbrStr);

            // lock the edge if both condition is met:
            //   + the neighbor is external, i.e. not in the same task. If it is in the same task
            //     then no other client will work on this neighbor
            //   + the neighbor has not been colored yet. If it has been colored, then no other
            //     client is working on this neighbor
            if((nbrId < firstNodeIdInSameTask) || (nbrId > lastNodeIdInSameTask)){
                // external
                numberOfLocks ++;
                iAmIsBorder = true;

            }
        }

        if(iAmIsBorder)
            numberOfBorderNodes ++;

    }


}
