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
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import static voldemort.VoldemortClientUtilities.*;
import static voldemort.VoldemortClientUtilities.getMinimalValueNotInArray;
import static voldemort.predicatedetection.debug.ExperimentVariables.*;

/**
 * Created by duongnn on 4/27/18.
 */
public class VoldemortClientGraphColoringClientLock {

    class ColoringTask{
        public static final int COLORING_TASK_STATUS_UNPROCESSED = 0;
        public static final int COLORING_TASK_STATUS_WORKING = 1;
        public static final int COLORING_TASK_STATUS_FINISHED = 2;

        int status;
        // list of locks who we have to obtain edge lock
        // suppose your id is 5, your client neighbors are 1 4 8
        // list of locks are 1_5, 4_5, 5_8
        Vector<ClientGraphPetersonLock> lockList;

        // list of nodes that we can safely color after all the locks in lockList are obtained
        Vector<String> nodeList;

        public ColoringTask(int status, Vector<ClientGraphPetersonLock> lockList, Vector<String> nodeList){
            this.status = status;
            this.lockList = lockList;
            this.nodeList = nodeList;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public Vector<ClientGraphPetersonLock> getLockList() {
            return lockList;
        }

        public void setLockList(Vector<ClientGraphPetersonLock> lockList) {
            this.lockList = lockList;
        }

        public Vector<String> getNodeList() {
            return nodeList;
        }

        public void setNodeList(Vector<String> nodeList) {
            this.nodeList = nodeList;
        }
    }

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

    // task assignment from file
    private String clientLowDegreeNodeAssignmentFileName;
    private Vector<ColoringTask> clientTaskList;

    private String highDegreeNodeColorFileName;
    private HashMap<String, String> highDegreeNodeColorMap;

    public VoldemortClientGraphColoringClientLock(VoldemortClientUtilities vcu,
                                        int clientId,
                                        int numberOfClients,
                                        String serverIp,
                                        String masterWaitProgIp,
                                        String masterWaitProgPortNumber,
                                        int runId,
                                        int numberOfOps,
                                        int taskSize,
                                        String clientLowDegreeNodeAssignmentFileName,
                                        String highDegreeNodeColorFileName) {

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
        this.clientLowDegreeNodeAssignmentFileName = clientLowDegreeNodeAssignmentFileName;

        // reading color of high degree nodes
        long startMs = System.currentTimeMillis();
        this.highDegreeNodeColorFileName = highDegreeNodeColorFileName;
        highDegreeNodeColorMap = new HashMap<>();
        File highDegreeNodeColorFile = new File(highDegreeNodeColorFileName);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(highDegreeNodeColorFile));
            String currentLine = reader.readLine();

            while(currentLine != null){
                int semicolonPos = currentLine.indexOf(':');
                String nodeIdStr = currentLine.substring(0, semicolonPos);
                String nodeColorStr = currentLine.substring(semicolonPos+1);

                highDegreeNodeColorMap.put(nodeIdStr, nodeColorStr);
                currentLine = reader.readLine();
            }
        }catch(IOException ioe){
            System.out.println(" Could not open or read file " + highDegreeNodeColorFileName);
            System.out.println(ioe.getMessage());
        }

        System.out.println("\n  finish reading high degree node color file in " +
                (System.currentTimeMillis() - startMs) + " ms");


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

        VoldemortClientGraphColoringClientLock voldClientGraph = new VoldemortClientGraphColoringClientLock(
                vcu,
                clientId,
                numberOfClients,
                bootstrapUrl,
                (String) options.valueOf("master-ip-addr"),
                (String) options.valueOf("master-wait-prog-waiting-port"),
                options.valueOf(runIdOptionSpec),
                options.valueOf(opCountSpec),
                options.valueOf(taskSizeSpec),
                (String) options.valueOf("node-assignment-file"),
                (String) options.valueOf("high-degree-node-color-filename"));

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
                        String.format("%%   number of tasks:            %d %n", numberOfTasks) +
                        String.format("%%   number of tasks completed:  %d %n", currentTaskId) +
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


        voldClientGraph.startClientGraphColoringClientLockProgram();

    }

    public void startClientGraphColoringClientLockProgram() throws Exception {
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


        // read task assignment from file into map
        readClientNodeAssignmentFromFile();

        // do coloring task
        performTasks();


        System.out.println("\n Client " + clientId + " finishes all the tasks in " +
                (System.nanoTime() - ExperimentVariables.experimentStartNs)/ Time.NS_PER_MS + " milliseconds");


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
                    String.format("%%   number of tasks:            %d %n", numberOfTasks) +
                    String.format("%%   number of tasks completed:  %d %n", currentTaskId) +
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

    void readClientNodeAssignmentFromFile() throws Exception{
        // open the graph input file
        File graphInputFile = new File(clientLowDegreeNodeAssignmentFileName + String.valueOf(clientId));
        BufferedReader reader = new BufferedReader(new FileReader(graphInputFile));
        String currentLine;

        clientTaskList = new Vector<>();

        // read file
        currentLine = reader.readLine();
        while(currentLine != null){
            // get lock list
            Vector<ClientGraphPetersonLock> lockList = null;
            int semicolonPosition = currentLine.indexOf(':');
            String neighborListStr = currentLine.substring(0, semicolonPosition);

            if(neighborListStr.trim().length() != 0){
                lockList = new Vector<>();
                String[] neighborList = neighborListStr.trim().split("\\s+");
                for(int i = 0; i < neighborList.length; i ++){
                    String clientIdStr = String.valueOf(clientId);
                    String nbrStr = neighborList[i];

                    if(clientId < Integer.valueOf(nbrStr)){
                        String bothNodeStr = clientIdStr + "_" + nbrStr;
                        lockList.add(
                                new ClientGraphPetersonLock(
                                        String.valueOf(clientId),
                                        neighborList[i],
                                        "flag" + bothNodeStr + "_" + clientIdStr,
                                        "flag" + bothNodeStr + "_" + nbrStr,
                                        "turn" + bothNodeStr
                        ));
                    }else{
                        String bothNodeStr = nbrStr + "_" + clientIdStr;
                        lockList.add(
                                new ClientGraphPetersonLock(
                                        String.valueOf(clientId),
                                        neighborList[i],
                                        "flag" + bothNodeStr + "_" + clientIdStr,
                                        "flag" + bothNodeStr + "_" + nbrStr,
                                        "turn" + bothNodeStr
                                ));
                    }
                }
            }

            // get node list
            String nodeListFullStr = currentLine.substring(semicolonPosition + 1);
            String[] nodeListFull = nodeListFullStr.trim().split("\\s+");

            Vector<String> nodeList = new Vector<>();

            for(int i = 0; i < nodeListFull.length; i++){
                nodeList.add(nodeListFull[i]);

//                // divide the node list into task size
//                if((nodeList.size() == taskSize) && (i < nodeListFull.length - 1)){
//                    // nodeList is full, but there is still more node
//                    clientTaskList.add(new ColoringTask(ColoringTask.COLORING_TASK_STATUS_UNPROCESSED, lockList, nodeList));
//
//                    nodeList = new Vector<>();
//                }
            }

            clientTaskList.add(new ColoringTask(ColoringTask.COLORING_TASK_STATUS_UNPROCESSED, lockList, nodeList));

            // move to next line
            currentLine = reader.readLine();
        }

        numberOfTasks = clientTaskList.size();

        // close input file
        reader.close();
    }

    void performTasks(){
        for(currentTaskId = 0; currentTaskId < clientTaskList.size(); currentTaskId ++){
            ColoringTask currentColoringTask = clientTaskList.elementAt(currentTaskId);

            Vector<ClientGraphPetersonLock> lockList = currentColoringTask.getLockList();
            Vector<String> nodeList = currentColoringTask.getNodeList();

            // Obtain the peterson locks if needed
            // Unlike VoldemortClientGraphColoring where we use getIndividualLockPetersonEnhanced
            // in which obtaining and informing locks are coupled together
            // In VoldemortClientGraphColoringClientLock,
            // if we use getIndividualLockPetersonEnhanced, we will create false positives
            // because it is a long time when client informing having the first lock \
            // to time when the last lock is obtained.
            // So we have to de-couple those 2 steps in this application

            if(lockList != null) {
                for (ClientGraphPetersonLock l : lockList) {
                    while(!voldClientUtilities.getIndividualLockPeterson(l, 0)){
                        System.out.println("getIndividualLockPeterson() return false");
                    }
                }

                // at this moment, it is OK to confirm that we are going to access critical section
                for(ClientGraphPetersonLock l : lockList){
                    // Duong debug
                    System.out.println(" ++ " + System.currentTimeMillis() + ": obtained lock " + l.getBothIdAndIdString());

                    voldClientUtilities.informIndividualLockPetersonObtained(l);
                }


            }

            // at this point, all necessary locks are obtained
            // we can color the nodes
            for(int nodeIndex = 0; nodeIndex < nodeList.size(); nodeIndex ++){
                assignColorForNode(nodeList.elementAt(nodeIndex));
            }

            // Release the locks
            if(lockList != null) {
                // since client is done with coloring (critical section), we can say
                // the critical section is over
                for(ClientGraphPetersonLock l : lockList){
                    voldClientUtilities.informIndividualLockPetersonReleased(l);

                    // Duong debug
                    System.out.println(" -- " + System.currentTimeMillis() + ": released lock " + l.getBothIdAndIdString());
                }

                for (ClientGraphPetersonLock l : lockList) {
                    // similarly to obtaining locks, we are not going to use enhanced version
                    voldClientUtilities.releaseIndividualLockPeterson(l);
                }
            }

        }
    }

    void assignColorForNode(String nodeIdStr){
        int nodeId = Integer.valueOf(nodeIdStr);

        // read from server color of node and list of node neighbors
        int originalColor = Integer.parseInt(voldClientUtilities.getVariable("v_" + nodeIdStr + "_col"));
        int newColor;
        String[] nodeNbrStrList = voldClientUtilities.getVariable("v_" + nodeIdStr + "_nbr").split("\\s+");
        int numberOfNbrs = nodeNbrStrList.length;
        int[] nbrColorList = new int[numberOfNbrs];

        // read colors of all neighbors
        for(int nbr = 0; nbr < numberOfNbrs; nbr++){
            String nbrStr = nodeNbrStrList[nbr];

            // check from high degree node list first
            String nbrColorStr;
            if((nbrColorStr = highDegreeNodeColorMap.get(nbrStr)) != null){
                nbrColorList[nbr] = Integer.parseInt(nbrColorStr.trim());
            }else {
                // query the server
                nbrColorList[nbr] = Integer.parseInt(voldClientUtilities.getVariable("v_" + nbrStr + "_col"));
            }
        }

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
    }



}
