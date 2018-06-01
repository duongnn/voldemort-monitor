package voldemort;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.w3c.dom.Element;
import predicatedetectionlib.common.XmlUtils;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
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

import voldemort.predicatedetection.ClientProxy;

import static voldemort.VoldemortClientUtilities.*;

/**
 * Created by duongnn on 8/2/17.
 */
public class VoldemortClientMeasurePerformance {
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
    private double beta;


    public VoldemortClientMeasurePerformance(VoldemortClientUtilities vcu,
                                             int clientId,
                                             String serverIp,
                                             String masterWaitProgIp,
                                             String masterWaitProgPortNumber,
                                             int runId,
                                             int numberOfOps,
                                             double putProportion,
                                             double beta) {

        voldClientUtilities = vcu;

        this.clientId = clientId;

        int slashPos = serverIp.lastIndexOf("/");
        this.serverIp = serverIp.substring(slashPos + 1);
        this.masterWaitProgIp = masterWaitProgIp;
        this.masterWaitProgPortNumber = masterWaitProgPortNumber;
        this.runId = runId;
        this.numberOfOps = numberOfOps;
        this.putProportion = putProportion;
        this.beta = beta;

        ExperimentVariables.serverIp = new String(serverIp);
        ExperimentVariables.runId = runId;
        ExperimentVariables.numberOfOps = numberOfOps;
        ExperimentVariables.mutableNumberOfOps = numberOfOps;

    }



    // program for measuring performance of Voldemort
    public void startMeasureProgram() throws Exception{

        // recording your thread so that the hook can stop you
        ExperimentVariables.mainClientThread = Thread.currentThread();

        ExperimentVariables.operationCount = 0;

        ExperimentVariables.experimentStartDateTime = DateFormat.getDateTimeInstance().format(System.currentTimeMillis());

        ExperimentVariables.experimentStartNs = System.nanoTime();

        System.out.println("\n\n *************************\n");
        System.out.println(" Starting clientMeasurePerformance program \n\n");

        // adjusting numberOfOps
        // if numberOfOps = 0, that means monitor will stop clients by SIGINT
        if(numberOfOps == 0) {
            ExperimentVariables.mutableNumberOfOps = Integer.MAX_VALUE;
        }

        //while (intervalCount < numberOfMeasureIntervals) {
        while(ExperimentVariables.operationCount < ExperimentVariables.mutableNumberOfOps){

            // run program to measure

//            // select random variable to write
//            int varIdUB = 100000 - 1;
//            int varIdLB = 0;
//            Random randGenerator = new Random(); // Random(intervalStartMs);

//            int varId = (int) ((ExperimentVariables.operationCount % (int)(10*1/beta)) + 1); // randGenerator.nextInt(varIdUB - varIdLB) + varIdLB;
//            String varName = "x"+varId;
            //String varName = "x0";

            // operationCount and putProportion will determine if this is a put or a get
            if(ExperimentVariables.operationCount % (int) (1/putProportion) == 0){
                // issue put/write

                // make sure a proportion of beta will be make local predicate be true
                double beta_of_put = beta/putProportion;
                int varId = (int) ((ExperimentVariables.operationCount % (int)(10*1/beta_of_put)) + 1); // randGenerator.nextInt(varIdUB - varIdLB) + varIdLB;
                String varName = "x"+varId;

                voldClientUtilities.setVariable(varName, "1");

            }else{
                // issue get/read
                int varId = (int) ((ExperimentVariables.operationCount % 10000) + 1);
                String varName = "x"+varId;

                voldClientUtilities.getVariable(varName);
            }

            ExperimentVariables.operationCount ++;

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
        if(clientId >= clientElementList.size()){
            System.out.println(" VoldemortClientMeasurePerformance.main() ERROR");
            System.out.println("  clientId (" + clientId + ") >= clientElementList.size (" + clientElementList.size() + ")");
            System.exit(-1);
        }

        // extracting arguments for this client from xml element
        String[] mainArg = null;
        for(int i = 0; i < clientElementList.size(); i++){
            Element clientElement = clientElementList.elementAt(i);
            String idStr = clientElement.getElementsByTagName("id").item(0).getTextContent();
            String mainArgStr = clientElement.getElementsByTagName("mainArg").item(0).getTextContent();

            if(clientId == Integer.valueOf(idStr)){
                mainArg = mainArgStr.split("\\s+");
                break;
            }
        }

        if(mainArg == null){
            System.out.println(" VoldemortClientMeasurePerformance.main() ERROR: mainArg is null");
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
                System.err.println("Usage: java VoldemortClientMeasurePerformance --client-id=0/1/2 --client-input-arg-file=file.xml");
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


        VoldemortClientMeasurePerformance voldMeasurePerformance = new VoldemortClientMeasurePerformance(vcu,
                clientId,
                bootstrapUrl,
                (String) options.valueOf("master-ip-addr"),
                (String) options.valueOf("master-wait-prog-waiting-port"),
                options.valueOf(runIdOptionSpec),
                options.valueOf(opCountSpec),
                options.valueOf(putProportionSpec),
                options.valueOf(betaSpec)
                );


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
                        String.format("%%   first timestamp: %,d %n", DebugPerformance.voldemort.getFirstTimestamp()) +
                        String.format("%%   last timestamp:  %,d %n", DebugPerformance.voldemort.getLastTimestamp()) +
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

        voldMeasurePerformance.startMeasureProgram();

    }


}
