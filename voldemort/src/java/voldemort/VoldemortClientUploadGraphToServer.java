package voldemort;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.w3c.dom.Element;
import predicatedetectionlib.common.XmlUtils;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.predicatedetection.ClientProxy;
import voldemort.predicatedetection.PredicateDetection;
import voldemort.predicatedetection.debug.DebugPerformance;
import voldemort.predicatedetection.debug.ExperimentVariables;
import voldemort.utils.*;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.DateFormat;
import java.util.*;

import static voldemort.VoldemortClientUtilities.*;

/**
 * Created by duongnn on 3/10/18.
 */
public class VoldemortClientUploadGraphToServer
{
//    // Duong debug
//    int maxNServer = 9;
//    int minNServer = 2;
//    Vector<Vector<Integer>> hashDistribution;


    // voldemort client library and some additional functionalities
    VoldemortClientUtilities voldClientUtilities;

    private int clientId;
    private String serverIp;

    private String masterWaitProgIp;
    private String masterWaitProgPortNumber;
    private int runId;
    private int numberOfOps;

    private String graphInputFileName;

    // status of a vertex in graph coloring
    protected static final String GRAPH_VERTEX_STATE_INACTIVE = "0";
    protected static final String GRAPH_VERTEX_STATE_ACTIVE = "1";

    // status of a client
    protected static final String CLIENT_STATE_IDLE = "0";
    protected static final String CLIENT_STATE_BUSY = "1";


    public VoldemortClientUploadGraphToServer(VoldemortClientUtilities vcu,
                                int clientId,
                                String serverIp,
                                String masterWaitProgIp,
                                String masterWaitProgPortNumber,
                                int runId,
                                int numberOfOps,
                                String graphInputFileName) {

        voldClientUtilities = vcu;

        this.clientId = clientId;

        int slashPos = serverIp.lastIndexOf("/");
        this.serverIp = serverIp.substring(slashPos + 1);
        this.masterWaitProgIp = masterWaitProgIp;
        this.masterWaitProgPortNumber = masterWaitProgPortNumber;
        this.runId = runId;
        this.numberOfOps = numberOfOps;
        this.graphInputFileName = graphInputFileName;

        ExperimentVariables.serverIp = new String(serverIp);
        ExperimentVariables.runId = runId;
        ExperimentVariables.numberOfOps = numberOfOps;
        ExperimentVariables.mutableNumberOfOps = numberOfOps;

    }


    // run client program to read file and upload to servers
    public void startClientUploadGraphProgram() throws Exception{

        // recording your thread so that the hook can stop you
        ExperimentVariables.mainClientThread = Thread.currentThread();

        ExperimentVariables.operationCount = 0;
        ExperimentVariables.numberOfNodesProcessed = 0;
        ExperimentVariables.insufficientOperationalNodesExceptionCount = 0;

        ExperimentVariables.experimentStartDateTime = DateFormat.getDateTimeInstance().format(System.currentTimeMillis());

        ExperimentVariables.experimentStartNs = System.nanoTime();

        System.out.println("\n\n *************************\n");
        System.out.println(" Starting clientUploadGraphToServer program \n\n");

        // adjusting numberOfOps
        // if numberOfOps = 0, that means monitor will stop clients by SIGINT
        if(numberOfOps == 0) {
            ExperimentVariables.mutableNumberOfOps = Integer.MAX_VALUE;
        }

        // only client 0 will upload graph info onto the servers
        if(clientId == 0) {
            System.out.println(" clientId = " + clientId + ": going to read graph file and upload");

            // open the input file
            File graphInputFile = new File(graphInputFileName);
            BufferedReader reader = new BufferedReader(new FileReader(graphInputFile));
            String currentLine;

            // read general info
            while(true){
                currentLine = reader.readLine();

                if(currentLine.charAt(0) == '#'){
                    break;
                }

                String[] currentLineSplit = currentLine.trim().split("=");
                switch(currentLineSplit[0]){
                    case "graphType":
                        // Duong debug
                        System.out.println("g_type = " + currentLineSplit[1]);

                        voldClientUtilities.setVariable("g_type",currentLineSplit[1]);
                        break;
                    case "numberOfNodes":
                        // Duong debug
                        System.out.println("g_numberOfNodes = " + currentLineSplit[1]);

                        voldClientUtilities.setVariable("g_numberOfNodes", currentLineSplit[1]);
                        break;
                    case "numberOfEdges":
                        // Duong debug
                        System.out.println("g_numberOfEdges = " + currentLineSplit[1]);

                        voldClientUtilities.setVariable("g_numberOfEdges", currentLineSplit[1]);
                        break;
                    case "maxDegree":
                        // Duong debug
                        System.out.println("g_maxDegree = " + currentLineSplit[1]);

                        voldClientUtilities.setVariable("g_maxDegree", currentLineSplit[1]);
                        break;
                    case "maxNumberOfColors":
                        // Duong debug
                        System.out.println("g_maxNumberOfColors = " + currentLineSplit[1]);

                        voldClientUtilities.setVariable("g_maxNumberOfColors", currentLineSplit[1]);
                        break;
                    default:
                        // Duong debug
                        System.out.println("unknown property: " + currentLineSplit[0] +
                                " and value: " + currentLineSplit[1]);

                }
            }

            // skip line starting with #
            while(currentLine.charAt(0) == '#'){
                currentLine = reader.readLine();
                if(currentLine == null){
                    System.out.println(" currentLine is null before reading adjacency list. Something wrong");
                    return;
                }
            }

//            // Duong debug
//            // We want to record the distribution of hashing edge
//            hashDistribution = new Vector<>(maxNServer - minNServer + 1);
//            for(int nServer = minNServer; nServer <= maxNServer; nServer ++){
//                Vector<Integer> v = new Vector<>(nServer);
//                for(int i = 0; i < nServer; i++){
//                    v.addElement(0);
//                }
//                hashDistribution.addElement(v);
//            }


            // read adjacent matrix
            while(currentLine != null){
                // get node id
                int spacePos = currentLine.indexOf(' ');
                String nodeId = currentLine.substring(0, spacePos);

                // get node neighbors
                String nodeNeighbors = currentLine.substring(spacePos+1);

                // Duong debug
//                System.out.println(" nodeId = " + nodeId);
//                System.out.println("     nodeNeighbors = " + nodeNeighbors);

                // write to server
                voldClientUtilities.setVariable("v_" + nodeId + "_nbr", nodeNeighbors);
                voldClientUtilities.setVariable("v_" + nodeId + "_col", nodeId);
                voldClientUtilities.setVariable("v_" + nodeId + "_ste", GRAPH_VERTEX_STATE_INACTIVE);

//                // Duong debug
//                String[] nbrList = nodeNeighbors.split("\\s+");
//                for(int i = 0; i < nbrList.length; i++){
//                    String nbr = nbrList[i];
//                    if(Integer.parseInt(nodeId) < Integer.parseInt(nbr)){
//                        String edge = nodeId + "_" + nbr;
//                        for(int nServer = minNServer; nServer <= maxNServer; nServer ++){
//                            int slot = Math.abs(edge.hashCode() % nServer);
//
//                            // Duong debug
////                            System.out.println("nServer = " + nServer + " minNServer = " + minNServer + " slot = " + slot);
////                            System.out.println("hashDistribution.size = " + hashDistribution.size()
////                                               + "hashDistribution.elementAt(" + (nServer-minNServer) + ").size = " +
////                                                hashDistribution.elementAt(nServer - minNServer).size());
//
//                            int currentCount = hashDistribution.elementAt(nServer - minNServer).elementAt(slot);
//                            hashDistribution.elementAt(nServer - minNServer).setElementAt(currentCount + 1, slot);
//                        }
//                    }
//                }


                // move to next node
                currentLine = reader.readLine();
            }


            // Duong debug
            System.out.println(" reach the end of graph input file in " +
                    (System.nanoTime() - ExperimentVariables.experimentStartNs)/1000000000 + " seconds");


//            // Duong debug
//            System.out.println("hashDistribution with mod:");
//            for(int nServer = minNServer; nServer <= maxNServer; nServer ++){
//                System.out.println(" nServer = " + nServer);
//                Vector<Integer> currentVector =hashDistribution.elementAt(nServer - minNServer);
//                for(int i = 0; i < currentVector.size(); i++){
//                    System.out.println("   i = " + i + ": " + currentVector.elementAt(i));
//                }
//            }

        }else{
            System.out.println(" clientId = " + clientId + ": do nothing");
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
        for(int i = 0; i < numberOfClients; i++){
            Element clientElement = clientElementList.elementAt(i);
            String idStr = clientElement.getElementsByTagName("id").item(0).getTextContent();
            String mainArgStr = clientElement.getElementsByTagName("mainArg").item(0).getTextContent();

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

        VoldemortClientUploadGraphToServer voldClientUploadGraph = new VoldemortClientUploadGraphToServer(vcu,
                clientId,
                bootstrapUrl,
                (String) options.valueOf("master-ip-addr"),
                (String) options.valueOf("master-wait-prog-waiting-port"),
                options.valueOf(runIdOptionSpec),
                options.valueOf(opCountSpec),
                (String) options.valueOf("graph-input-file-name"));


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
                        String.format("%%   Client Id:                  %s %n", clientId) +
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


        voldClientUploadGraph.startClientUploadGraphProgram();

    }


}

