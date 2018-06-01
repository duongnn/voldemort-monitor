package voldemort;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import joptsimple.OptionParser;
import joptsimple.OptionSpec;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.lang.mutable.MutableInt;
import predicatedetectionlib.common.clientgraph.ClientGraphPetersonLock;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.predicatedetection.debug.DebugPerformance;
import voldemort.predicatedetection.debug.ExperimentVariables;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.json.EndOfFileException;
import voldemort.serialization.json.JsonReader;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.utils.*;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.DateFormat;
import java.util.*;

/**
 * Created by duongnn on 3/12/18.
 */
public class VoldemortClientUtilities {

    ///////////////////////////////////////////////////////////////////////////////
    // 1. Option parsing
    ///////////////////////////////////////////////////////////////////////////////

    public static OptionSpec<Integer> breakTimeOption;
    public static OptionSpec<Boolean> predDetectionOption;
    public static OptionSpec<Integer> runIdOptionSpec;
    public static OptionSpec<Boolean> proxyEnableOptionSpec;
    public static OptionSpec<String> proxyBasePortNumberSpec;
    public static OptionSpec<Integer> opCountSpec;
    public static OptionSpec<Double> putProportionSpec;
    public static OptionSpec<Integer> incrementStepOptionSpec;
    public static OptionSpec<Integer> lineGraphLengthPerClientSpec;
    public static OptionSpec<Double> betaSpec;
    public static OptionSpec<Integer> taskSizeSpec;
    public static OptionSpec<Boolean> readNeighborColorByGetallSpec;
    public static OptionSpec<Boolean> preprocessHighDegreNodesSpec;

    public static void parserAcceptanceConfig(OptionParser parser){
        parser.accepts("client-zone-id", "Client zone id for zone routing")
                .withRequiredArg()
                .describedAs("zone-id")
                .ofType(Integer.class);
        parser.accepts("config-file", "A properties file that contains client config properties")
                .withRequiredArg()
                .describedAs("file");
        parser.accepts("help", "Print this help message")
                .isForHelp();
        breakTimeOption = parser.accepts("break-time", "break time between commands in app")
                .withOptionalArg()
                .ofType(Integer.class)
                .defaultsTo(0);
        predDetectionOption = parser.accepts("predicate-detection", "enable predicate detection or not")
                .withOptionalArg()
                .describedAs("pred-detection")
                .ofType(Boolean.class)
                .defaultsTo(true);
        parser.accepts("master-ip-addr", "IP address of program at master node that wait for signal to stop experiments")
                .withRequiredArg()
                .ofType(String.class);
        parser.accepts("master-wait-prog-waiting-port", "IP port number of program at master node that wait for signal to stop experiments")
                .withRequiredArg()
                .ofType(String.class);
        runIdOptionSpec = parser.accepts("run-id", "sequence number of this run")
                .withRequiredArg()
                .ofType(Integer.class);
        proxyEnableOptionSpec = parser.accepts("proxy-enable", "is proxy used?")
                .withOptionalArg()
                .ofType(Boolean.class)
                .defaultsTo(false);
        proxyBasePortNumberSpec = parser.accepts("proxy-base-local-port")
                .withOptionalArg()
                .ofType(String.class);
        opCountSpec = parser.accepts("operation-count", "number of operations to run")
                .withRequiredArg()
                .ofType(Integer.class);
        putProportionSpec = parser.accepts("put-proportion")
                .withRequiredArg()
                .ofType(Double.class);
        betaSpec = parser.accepts("beta")
                .withRequiredArg()
                .ofType(Double.class);
        incrementStepOptionSpec = parser.accepts("increment-step", "interval to write out measurements")
                .withOptionalArg()
                .ofType(Integer.class)
                .defaultsTo(1);
        lineGraphLengthPerClientSpec = parser.accepts("line-graph-length-per-client")
                .withRequiredArg()
                .ofType(Integer.class);
        parser.accepts("graph-input-file-name")
                .withOptionalArg();
        taskSizeSpec = parser.accepts("task-size")
                .withOptionalArg()
                .ofType(Integer.class);
        preprocessHighDegreNodesSpec = parser.accepts("preprocess-high-degree-nodes")
                .withRequiredArg()
                .ofType(Boolean.class)
                .defaultsTo(true);
        readNeighborColorByGetallSpec = parser.accepts("read-neighbor-color-by-getall")
                .withRequiredArg()
                .ofType(Boolean.class)
                .defaultsTo(false);

        parser.accepts("node-assignment-file")
                .withRequiredArg()
                .defaultsTo("../voldemort-related/programs/PreprocessGraph/lowDegreeNodeAssignment_client_");
        parser.accepts("high-degree-node-color-filename")
                .withRequiredArg()
                .defaultsTo("../voldemort-related/programs/PreprocessGraph/highDegreeNodesColor.txt");

    }

    ///////////////////////////////////////////////////////////////////////////////
    // 2. voldemort client library that provides functionalities for application
    ///////////////////////////////////////////////////////////////////////////////

    protected static final String PROMPT = "> ";

    protected StoreClient<Object, Object> client;

    private SocketStoreClientFactory factory;

    private StoreDefinition storeDef;

    private RoutingStrategy routingStrategy;

    protected final BufferedReader commandReader;

    protected final PrintStream commandOutput;

    protected final PrintStream errorStream;

    private AdminClient adminClient;

//    protected VoldemortClientUtilities(BufferedReader commandReader,
//                                   PrintStream commandOutput,
//                                   PrintStream errorStream) {
//        this.commandReader = commandReader;
//        this.commandOutput = commandOutput;
//        this.errorStream = errorStream;
//    }

    public VoldemortClientUtilities(ClientConfig clientConfig,
                                String storeName,
                                BufferedReader commandReader,
                                PrintStream commandOutput,
                                PrintStream errorStream) {

        this.commandReader = commandReader;
        this.commandOutput = commandOutput;
        this.errorStream = errorStream;

        try {
            factory = new SocketStoreClientFactory(clientConfig);
            client = factory.getStoreClient(storeName);
            adminClient = new AdminClient(clientConfig.setIdentifierString("admin"));

            storeDef = StoreUtils.getStoreDef(factory.getStoreDefs(), storeName);

            Cluster cluster = adminClient.getAdminClientCluster();
            routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);

            commandOutput.println("Established connection to " + storeName + " via "
                    + Arrays.toString(clientConfig.getBootstrapUrls()));
            commandOutput.print(PROMPT);
        } catch(Exception e) {
            safeClose();
            Utils.croak("Could not connect to server: " + e.getMessage());
        }
    }

    // getter method for the Store
    public StoreClient<Object, Object> getStoreClient() {
        return this.client;
    }

    protected void safeClose() {
        if(adminClient != null)
            adminClient.close();
        if(factory != null)
            factory.close();
    }

    public void process(boolean printCommands) {
        try {
            processCommands(printCommands);
        } catch(Exception e) {
            Utils.croak("Error processing commands.." + e.getMessage());
        } finally {
            safeClose();
        }
    }


    public static Object parseObject(SerializerDefinition serializerDef,
                                     String argStr,
                                     MutableInt parsePos,
                                     PrintStream errorStream) {
        Object obj = null;
        try {
            // TODO everything is read as json string now..
            JsonReader jsonReader = new JsonReader(new StringReader(argStr));
            obj = jsonReader.read();
            // mark how much of the original string, we blew through to
            // extract the avrostring.
            parsePos.setValue(jsonReader.getCurrentLineOffset() - 1);

            if(StoreDefinitionUtils.isAvroSchema(serializerDef.getName())) {
                // TODO Need to check all the avro siblings work
                // For avro, we hack and extract avro key/value as a string,
                // before we do the actual parsing with the schema
                String avroString = (String) obj;
                // From here on, this is just normal avro parsing.
                Schema latestSchema = Schema.parse(serializerDef.getCurrentSchemaInfo());
                try {
                    JsonDecoder decoder = new JsonDecoder(latestSchema, avroString);
                    GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(latestSchema);
                    obj = datumReader.read(null, decoder);
                } catch(IOException io) {
                    errorStream.println("Error parsing avro string " + avroString);
                    io.printStackTrace();
                }
            } else {
                // all json processing does some numeric type tightening
                obj = tightenNumericTypes(obj);
            }
        } catch(EndOfFileException eof) {
            // can be thrown from the jsonReader.read(..) call indicating, we
            // have nothing more to read.
            obj = null;
        }
        return obj;
    }

    protected Object parseKey(String argStr, MutableInt parsePos) {
        return parseObject(storeDef.getKeySerializer(), argStr, parsePos, this.errorStream);
    }

    protected Object parseValue(String argStr, MutableInt parsePos) {
        return parseObject(storeDef.getValueSerializer(), argStr, parsePos, this.errorStream);
    }

    protected byte[] serializeKey(Object key) {
        SerializerFactory serializerFactory = factory.getSerializerFactory();
        SerializerDefinition serializerDef = storeDef.getKeySerializer();
        Serializer<Object> keySerializer = (Serializer<Object>) serializerFactory.getSerializer(serializerDef);
        return keySerializer.toBytes(key);
    }

    protected void processPut(String putArgStr) {
        MutableInt parsePos = new MutableInt(0);
        Object key = parseKey(putArgStr, parsePos);
        putArgStr = putArgStr.substring(parsePos.intValue());
        Object value = parseValue(putArgStr, parsePos);
        client.put(key, value);
    }


    /**
     *
     * @param getAllArgStr space separated list of key strings
     */
    protected void processGetAll(String getAllArgStr) {
        List<Object> keys = new ArrayList<Object>();
        MutableInt parsePos = new MutableInt(0);

        while(true) {
            Object key = parseKey(getAllArgStr, parsePos);
            if(key == null) {
                break;
            }
            keys.add(key);
            getAllArgStr = getAllArgStr.substring(parsePos.intValue());
        }

        Map<Object, Versioned<Object>> vals = client.getAll(keys);
        if(vals.size() > 0) {
            for(Map.Entry<Object, Versioned<Object>> entry: vals.entrySet()) {
                commandOutput.print(entry.getKey());
                commandOutput.print(" => ");
                printVersioned(entry.getValue());
            }
        } else {
            commandOutput.println("null");
        }
    }

    protected void processGet(String getArgStr) {
        MutableInt parsePos = new MutableInt(0);
        Object key = parseKey(getArgStr, parsePos);
        printVersioned(client.get(key));
    }

    protected void processPreflist(String preflistArgStr) {
        MutableInt parsePos = new MutableInt(0);
        Object key = parseKey(preflistArgStr, parsePos);
        byte[] serializedKey = serializeKey(key);
        printPartitionList(routingStrategy.getPartitionList(serializedKey));
        printNodeList(routingStrategy.routeRequest(serializedKey), factory.getFailureDetector());
    }

    protected void processDelete(String deleteArgStr) {
        MutableInt parsePos = new MutableInt(0);
        Object key = parseKey(deleteArgStr, parsePos);
        client.delete(key);
    }

    protected void processCommands(boolean printCommands) throws IOException {
        for(String line = commandReader.readLine(); line != null; line = commandReader.readLine()) {
            if(line.trim().equals("")) {
                commandOutput.print(PROMPT);
                continue;
            }
            if(printCommands)
                commandOutput.println(line);

            evaluateCommand(line, printCommands);

            commandOutput.print(PROMPT);
        }
    }

    // useful as this separates the repeated prompt from the evaluation
    // using no modifier as no sub-class will have access but all classes within
    // package will
    boolean evaluateCommand(String line, boolean printCommands) {
        try {
            if (line.toLowerCase().startsWith("put")) {
                processPut(line.substring("put".length()));
            } else if(line.toLowerCase().startsWith("getall")) {
                processGetAll(line.substring("getall".length()));
            } else if(line.toLowerCase().startsWith("getmetadata")) {
                String[] args = line.substring("getmetadata".length() + 1).split("\\s+");
                int remoteNodeId = Integer.valueOf(args[0]);
                String key = args[1];
                Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(remoteNodeId,
                        key);
                if(versioned == null) {
                    commandOutput.println("null");
                } else {
                    commandOutput.println(versioned.getVersion());
                    commandOutput.print(": ");
                    commandOutput.println(versioned.getValue());
                    commandOutput.println();
                }
            } else if(line.toLowerCase().startsWith("get")) {
                processGet(line.substring("get".length()));
            } else if(line.toLowerCase().startsWith("delete")) {
                processDelete(line.substring("delete".length()));
            } else if(line.startsWith("preflist")) {
                processPreflist(line.substring("preflist".length()));
            } else if(line.toLowerCase().startsWith("fetchkeys")) {
                String[] args = line.substring("fetchkeys".length() + 1).split("\\s+");
                int remoteNodeId = Integer.valueOf(args[0]);
                String storeName = args[1];
                List<Integer> partititionList = parseCsv(args[2]);
                Iterator<ByteArray> partitionKeys = adminClient.bulkFetchOps.fetchKeys(remoteNodeId,
                        storeName,
                        partititionList,
                        null,
                        false);

                BufferedWriter writer = null;
                try {
                    if(args.length > 3) {
                        writer = new BufferedWriter(new FileWriter(new File(args[3])));
                    } else
                        writer = new BufferedWriter(new OutputStreamWriter(commandOutput));
                } catch(IOException e) {
                    errorStream.println("Failed to open the output stream");
                    e.printStackTrace(errorStream);
                }
                if(writer != null) {
                    while(partitionKeys.hasNext()) {
                        ByteArray keyByteArray = partitionKeys.next();
                        StringBuilder lineBuilder = new StringBuilder();
                        lineBuilder.append(ByteUtils.getString(keyByteArray.get(), "UTF-8"));
                        lineBuilder.append("\n");
                        writer.write(lineBuilder.toString());
                    }
                    writer.flush();
                }
            } else if(line.toLowerCase().startsWith("fetch")) {
                String[] args = line.substring("fetch".length() + 1).split("\\s+");
                int remoteNodeId = Integer.valueOf(args[0]);
                String storeName = args[1];
                List<Integer> partititionList = parseCsv(args[2]);
                Iterator<Pair<ByteArray, Versioned<byte[]>>> partitionEntries = adminClient.bulkFetchOps.fetchEntries(remoteNodeId,
                        storeName,
                        partititionList,
                        null,
                        false);
                BufferedWriter writer = null;
                try {
                    if(args.length > 3) {
                        writer = new BufferedWriter(new FileWriter(new File(args[3])));
                    } else
                        writer = new BufferedWriter(new OutputStreamWriter(commandOutput));
                } catch(IOException e) {
                    errorStream.println("Failed to open the output stream");
                    e.printStackTrace(errorStream);
                }
                if(writer != null) {
                    while(partitionEntries.hasNext()) {
                        Pair<ByteArray, Versioned<byte[]>> pair = partitionEntries.next();
                        ByteArray keyByteArray = pair.getFirst();
                        Versioned<byte[]> versioned = pair.getSecond();
                        StringBuilder lineBuilder = new StringBuilder();
                        lineBuilder.append(ByteUtils.getString(keyByteArray.get(), "UTF-8"));
                        lineBuilder.append("\t");
                        lineBuilder.append(versioned.getVersion());
                        lineBuilder.append("\t");
                        lineBuilder.append(ByteUtils.getString(versioned.getValue(), "UTF-8"));
                        lineBuilder.append("\n");
                        writer.write(lineBuilder.toString());
                    }
                    writer.flush();
                }
            } else if(line.startsWith("help")) {
                commandOutput.println();
                commandOutput.println("Commands:");
                commandOutput.println(PROMPT
                        + "put key value --- Associate the given value with the key.");
                commandOutput.println(PROMPT
                        + "get key --- Retrieve the value associated with the key.");
                commandOutput.println(PROMPT
                        + "getall key1 [key2...] --- Retrieve the value(s) associated with the key(s).");
                commandOutput.println(PROMPT
                        + "delete key --- Remove all values associated with the key.");
                commandOutput.println(PROMPT
                        + "preflist key --- Get node preference list for given key.");
                String metaKeyValues = voldemort.store.metadata.MetadataStore.METADATA_KEYS.toString();
                commandOutput.println(PROMPT
                        + "getmetadata node_id meta_key --- Get store metadata associated "
                        + "with meta_key from node_id. meta_key may be one of "
                        + metaKeyValues.substring(1, metaKeyValues.length() - 1)
                        + ".");
                commandOutput.println(PROMPT
                        + "fetchkeys node_id store_name partitions <file_name> --- Fetch all keys "
                        + "from given partitions (a comma separated list) of store_name on "
                        + "node_id. Optionally, write to file_name. "
                        + "Use getmetadata to determine appropriate values for store_name and partitions");
                commandOutput.println(PROMPT
                        + "fetch node_id store_name partitions <file_name> --- Fetch all entries "
                        + "from given partitions (a comma separated list) of store_name on "
                        + "node_id. Optionally, write to file_name. "
                        + "Use getmetadata to determine appropriate values for store_name and partitions");
                commandOutput.println(PROMPT + "help --- Print this message.");
                commandOutput.println(PROMPT + "exit --- Exit from this shell.");
                commandOutput.println();
                commandOutput.println("Avro usage:");
                commandOutput.println("For avro keys or values, ensure that the entire json string is enclosed within single quotes (').");
                commandOutput.println("Also, the field names and strings should STRICTLY be enclosed by double quotes(\")");
                commandOutput.println("eg: > put '{\"id\":1,\"name\":\"Vinoth Chandar\"}' '[{\"skill\":\"java\", \"score\":90.27, \"isendorsed\": true}]'");

            } else if(line.equals("quit") || line.equals("exit")) {
                commandOutput.println("bye.");
                System.exit(0);
            } else {
                errorStream.println("Invalid command. (Try 'help' for usage.)");
                return false;
            }
        } catch(EndOfFileException e) {
            errorStream.println("Expected additional token.");
        } catch(SerializationException e) {
            errorStream.print("Error serializing values: ");
            e.printStackTrace(errorStream);
        } catch(VoldemortException e) {
//            // Duong debug
//            errorStream.println("Exception thrown during operation.");
//            e.printStackTrace(errorStream);

            // throw exception we already know in advance so that upper layer application
            // will process the exception
            if(e instanceof ObsoleteVersionException){
                throw e;
            }else if (e instanceof InsufficientOperationalNodesException){
                throw e;
            }else {
                errorStream.println("Exception thrown during operation.");
                e.printStackTrace(errorStream);
            }
        } catch(ArrayIndexOutOfBoundsException e) {
            errorStream.println("Invalid command. (Try 'help' for usage.)");
        } catch(Exception e) {
            errorStream.println("Unexpected error:");
            e.printStackTrace(errorStream);
        }
        return true;
    }

    protected List<Integer> parseCsv(String csv) {
        return Lists.transform(Arrays.asList(csv.split(",")), new Function<String, Integer>() {

            public Integer apply(String input) {
                return Integer.valueOf(input);
            }
        });
    }

    private void printNodeList(List<Node> nodes, FailureDetector failureDetector) {
        if(nodes.size() > 0) {
            for(int i = 0; i < nodes.size(); i++) {
                Node node = nodes.get(i);
                commandOutput.println("Node " + node.getId());
                commandOutput.println("host:  " + node.getHost());
                commandOutput.println("port: " + node.getSocketPort());
                commandOutput.println("available: "
                        + (failureDetector.isAvailable(node) ? "yes" : "no"));
                commandOutput.println("last checked: " + failureDetector.getLastChecked(node)
                        + " ms ago");
                commandOutput.println();
            }
        }
    }

    private void printPartitionList(List<Integer> partitions) {
        commandOutput.println("Partitions:");
        for (Integer partition: partitions) {
            commandOutput.println("    " + partition.toString());
        }
    }

    protected void printVersioned(Versioned<Object> v) {
        if(v == null) {
            commandOutput.println("null");
        } else {
            commandOutput.print(v.getVersion());
            commandOutput.print(": ");
            printObject(v.getValue());
            commandOutput.println();
        }
    }

    @SuppressWarnings("unchecked")
    protected void printObject(Object o) {
        if(o == null) {
            commandOutput.print("null");
        } else if(o instanceof String) {
            commandOutput.print('"');
            commandOutput.print(o);
            commandOutput.print('"');
        } else if(o instanceof Date) {
            DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
            commandOutput.print("'");
            commandOutput.print(df.format((Date) o));
            commandOutput.print("'");
        } else if(o instanceof List) {
            List<Object> l = (List<Object>) o;
            commandOutput.print("[");
            for(Object obj: l)
                printObject(obj);
            commandOutput.print("]");
        } else if(o instanceof Map) {
            Map<Object, Object> m = (Map<Object, Object>) o;
            commandOutput.print('{');
            for(Object key: m.keySet()) {
                printObject(key);
                commandOutput.print(':');
                printObject(m.get(key));
                commandOutput.print(", ");
            }
            commandOutput.print('}');
        } else if(o instanceof Object[]) {
            Object[] a = (Object[]) o;
            commandOutput.print(Arrays.deepToString(a));
        } else if(o instanceof byte[]) {
            byte[] a = (byte[]) o;
            commandOutput.print(Arrays.toString(a));
        } else if(o instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) o;
            commandOutput.print(ByteUtils.toHexString(buffer.array()));
        } else {
            commandOutput.print(o);
        }
    }

    /*
     * We need to coerce numbers to the tightest possible type and let the
     * schema coerce them to the proper
     */
    @SuppressWarnings("unchecked")
    public static Object tightenNumericTypes(Object o) {
        if(o == null) {
            return null;
        } else if(o instanceof List) {
            List l = (List) o;
            for(int i = 0; i < l.size(); i++)
                l.set(i, tightenNumericTypes(l.get(i)));
            return l;
        } else if(o instanceof Map) {
            Map m = (Map) o;
            for(Map.Entry entry: (Set<Map.Entry>) m.entrySet())
                m.put(entry.getKey(), tightenNumericTypes(entry.getValue()));
            return m;
        } else if(o instanceof Number) {
            Number n = (Number) o;
            if(o instanceof Integer) {
                if(n.intValue() < Byte.MAX_VALUE)
                    return n.byteValue();
                else if(n.intValue() < Short.MAX_VALUE)
                    return n.shortValue();
                else
                    return n;
            } else if(o instanceof Double) {
                if(n.doubleValue() < Float.MAX_VALUE)
                    return n.floatValue();
                else
                    return n;
            } else {
                throw new RuntimeException("Unsupported numeric type: " + o.getClass());
            }
        } else {
            return o;
        }
    }


    ///////////////////////////////////////////////////////////////////////////////
    // 3. Non-static Utility functions since they need information from the library
    ///////////////////////////////////////////////////////////////////////////////

    // perform put/write operation
    public boolean setVariable(String varName, String varValue){

//        // duong debug
//        System.out.println("setVariable(): " + varName);

        while(true) {
            long startNs;
            long durationNs;
            try {
                startNs = System.nanoTime();
                evaluateCommand("put \"" + varName + "\" \"" + varValue + "\"", true);
                durationNs = System.nanoTime() - startNs;
            } catch (ObsoleteVersionException ove) {
//                // Duong debug
//                System.out.println("   setVariable(): get ObsoleteVersionException " + ove.getMessage());

                continue;
            } catch (InsufficientOperationalNodesException ione){
                // Duong debug
//                System.out.println("   setVariable(): get InsufficientOperationalNodesException");

                // Duong debug
                ExperimentVariables.insufficientOperationalNodesExceptionCount ++;

                // make another attempt
                continue;
            } catch (Exception e){

                // Duong debug
                System.out.println(" setVariable(): there are general exceptions " + e.getMessage());
                continue;
            }

            // count the operation
            ExperimentVariables.operationCount ++;

            // record operation using RequestCounter
            ExperimentVariables.appCounterForPut.
                    addRequest(durationNs,
                            0,
                            varValue.length(),
                            varName.length(),
                            0);

            // retrieve recorded information too
            DebugPerformance.application.recordPerformance(System.currentTimeMillis(),
                    ExperimentVariables.appCounterForPut.getThroughput(),
                    ExperimentVariables.appCounterForPut.getAverageTimeInMs(),
                    ExperimentVariables.appCounterForGet.getThroughput(),
                    ExperimentVariables.appCounterForGet.getAverageTimeInMs());

            return true;
        }
    }

    // perform get/read operation
    // Note that client.get(key) could return
    //  + InsufficientZoneResponsesException
    //  + InsufficientOperationalNodesException:
    //  + InconsistentDataException: means more than one values are returned
    //    and those values could not be resolved.
    //  + null: means the requested key is not initialized yet.
    //  + single value: normal case.
    // This function return either
    //      null: when variable is not yet initialized
    //      single value: normal case
    // Note syntax of get in Voldemort is
    //     get "varname"
    //
    public String getVariable(String varName){

//        // duong debug
//        System.out.println("getVariable(): " + varName);

        Object key = parseKey("\"" + varName + "\"", new MutableInt(0));

        while(true) {
            String results;
            long startNs = System.nanoTime();

            Versioned<Object> value;

            try {
                value = client.get(key);
            } catch (InsufficientOperationalNodesException ione) {

//                System.out.println(" getVariable(): get InsufficientOperationalNodesException " + ione.getMessage());

                ExperimentVariables.insufficientOperationalNodesExceptionCount++;
                continue;
            } catch (InconsistentDataException ide) {
                System.out.println(" getVariable(): get InconsistentDataException " + ide.getMessage());
                continue;
            } catch (InsufficientZoneResponsesException izre) {
                System.out.println(" getVariable(): get InsufficientZoneResponsesException " + izre.getMessage());
                continue;
            }

            if (value != null) {
                results = (String) value.getValue();
            } else {
                // what does null mean?
                // it means client variable is not set
                // if client is set, even with empty string value, then it will not be null.
                // in case timeout happened and client did not get enough responses from servers
                // we should have had InsufficientOperationalNodesException

                //results = "";
                results = null;
            }

            long durationNs = System.nanoTime() - startNs;

            // count the operation
            ExperimentVariables.operationCount++;

            // record operation using RequestCounter
            ExperimentVariables.appCounterForGet.
                    addRequest(durationNs,
                            results == null ? 1 : 0,
                            results == null ? 0 : results.length(),
                            varName.length(),
                            0);

            // retrieve recorded information too
            DebugPerformance.application.recordPerformance(System.currentTimeMillis(),
                    ExperimentVariables.appCounterForPut.getThroughput(),
                    ExperimentVariables.appCounterForPut.getAverageTimeInMs(),
                    ExperimentVariables.appCounterForGet.getThroughput(),
                    ExperimentVariables.appCounterForGet.getAverageTimeInMs());

            return results;
        }
    }

    /**
     * get the values of multiple variables in one request
     * note: syntax of getall in voldemort is
     *      getall "var1" "var2" "var3"
     * @param getAllArgStr
     * @return
     *      + null: if all variables specified are uninitialized
     *      + a (hash) map of variable names and their values otherwise
     */
    public HashMap<String, String> getAllVariables(String getAllArgStr){
        List<Object> keys = new ArrayList<Object>();
        MutableInt parsePos = new MutableInt(0);

        while(true) {
            Object key = parseKey(getAllArgStr, parsePos);
            if(key == null) {
                break;
            }
            keys.add(key);
            getAllArgStr = getAllArgStr.substring(parsePos.intValue());
        }

        while(true) {
            Map<Object, Versioned<Object>> vals;
            HashMap<String, String> results = new HashMap<>();

            try {
                vals = client.getAll(keys);
            } catch (InsufficientOperationalNodesException ione) {
                ExperimentVariables.insufficientOperationalNodesExceptionCount++;
                continue;
            } catch (InconsistentDataException ide) {
                System.out.println(" getVariable(): get InconsistentDataException " + ide.getMessage());
                continue;
            } catch (InsufficientZoneResponsesException izre) {
                System.out.println(" getVariable(): get InsufficientZoneResponsesException " + izre.getMessage());
                continue;
            }

            if (vals.size() > 0) {
                for (Map.Entry<Object, Versioned<Object>> entry : vals.entrySet()) {
                    results.put(
                            (String) entry.getKey(),
                            (String) entry.getValue().getValue());
                }
            } else {
                results = null;
            }

            return results;
        }
    }

    /**
     * Obtain an individual lock in Peterson algorithm
     * @param lock          the peterson lock
     * @param timeOutNs       if > 0, could return without obtaining the lock
     *                        <=0 means no timeout (could wait indefinitely)
     * @return  true if lock is obtained
     *          false if timeout or some error happens
     */
    boolean getIndividualLockPeterson(ClientGraphPetersonLock lock, long timeOutNs){
        return getIndividualLockPeterson(
                lock.getNodeFlagVar(),
                lock.getNeighborFlagVar(),
                lock.getTurnVar(),
                lock.getNodeName(),
                timeOutNs);
    }

    /**
     * Enhanced version for obtaining individual Peterson Lock
     * The enhancement is that the client will issue one more write requests explicitly saying it is going to use the critical section
     * This enhancement does not benefit the clients/applications
     * However, it help the monitor to monitor the mutual exclusion predicate better. In particular, it helps reducing
     * the false positive for the monitors
     *
     * @param lock          the peterson lock
     * @param timeOutNs       if > 0, could return without obtaining the lock
     *                        <=0 means no timeout (could wait indefinitely)
     * @return  true if lock is obtained
     *          false if timeout or some error happens
     */
    boolean getIndividualLockPetersonEnhanced(ClientGraphPetersonLock lock, long timeOutNs) {
        if(!getIndividualLockPeterson(lock, timeOutNs)){
            return false;
        }

        // now client have the privilege to access critical section
        // It just need to issue one write request to reflect that usage in server states so that
        // monitor can detect predicate more accurately
        informIndividualLockPetersonObtained(lock);

        // we are assuming setVariable is always successful
        return true;

    }

    /**
     * update state of server to reflect that the client is going to access
     * critical section related to/controlled by lock
     * The purpose of this extra step is to reduce false positives
     * @param lock
     */
    void informIndividualLockPetersonObtained(ClientGraphPetersonLock lock){
        String prefix = "ecs"; // edge critical section
        String actionVariable = prefix + lock.getBothIdAndIdString();
        setVariable(actionVariable, "1");
    }

    /**
     * Obtain an individual lock in Peterson algorithm
     * Says process 5 want to obtain the lock between process 5 and process 6
     * @param myFlagVar     flag5_6_5
     * @param otherFlagVar  flag5_6_6
     * @param turnVar       turn5_6
     * @param turnVarValue  5
     * @param timeOutNs       if > 0, could return without obtaining the lock
     *                        <=0 means no timeout (could wait indefinitely)
     * @return  true if lock is obtained
     *          false if timeout or some error happens
     */
    boolean getIndividualLockPeterson(String myFlagVar, String otherFlagVar, String turnVar, String turnVarValue, long timeOutNs){
        long startNs = System.nanoTime();

//        System.out.println("   getInvidualLockPeterson:");
//        System.out.println("    myFlagVar:      " + myFlagVar);
//        System.out.println("    otherFlagVar:   " + otherFlagVar);
//        System.out.println("    turnVar:        " + turnVar);
//        System.out.println("    turnVarValue:   " + turnVarValue);
//        System.out.println("    timeout:        " + timeOutNs);

        while(true){
            try{
                // no need to clear my flag, just set my flag immediately
                setVariable(myFlagVar, "1");

                // set turn
                setVariable(turnVar, turnVarValue);

                // read other flag var
                String otherFlagValueStr = getVariable(otherFlagVar);

                // read turn var
                String turnVarNewValueStr = getVariable(turnVar);

                if ((timeOutNs > 0) && (System.nanoTime() - startNs >= timeOutNs))
                    return false;

                // busy wait if needed
                boolean timeIsOut = false;

//                while(otherFlagValueStr==null ||        // read did not succeed. No, it means var not initialized
//                        turnVarNewValueStr == null ||   // read did not succeed. Should not happen
//                        (otherFlagValueStr.equals("1") && turnVarNewValueStr.equals(turnVarValue))
//                     ){

                while (otherFlagValueStr != null
                        && otherFlagValueStr.equals("1")
                        && turnVarNewValueStr.equals(turnVarValue)
                        ) {

                    // check if timeout?
                    if (timeOutNs > 0) {
                        if (System.nanoTime() - startNs >= timeOutNs) {
                            timeIsOut = true;
                            break;
                        }
                    }

                    // read other's flag var
                    otherFlagValueStr = getVariable(otherFlagVar);

                    // read turn var
                    turnVarNewValueStr = getVariable(turnVar);
                }

                // when we get to this point, either time is out or busy wait is completed
                if(timeIsOut){
                    return false;
                }else{
                    return true;
                }

            }catch(VoldemortException ve){
                // Duong debug
                System.out.println(" getIndividualLockPeterson: get exception " + ve.getMessage() );
                System.out.println(" will return false");
                ve.printStackTrace();
                System.out.println();


                // should you return false?
                return false;
            }
        }
    }

    /**
     * The counter part of getIndividualLockPetersonEnhanced
     * @param lock
     * @return
     */
    boolean releaseIndividualLockPetersonEnhanced(ClientGraphPetersonLock lock){
        // the order is reverse the getIndividualLockPetersonEnhanced

        // now client have finished using critical section
        // It just need to issue one write request to reflect that completion in server states so that
        // monitor can detect predicate more accurately
        informIndividualLockPetersonReleased(lock);

        // release actual locking variables
        return releaseIndividualLockPeterson(lock);
    }

    /**
     * update state of server to reflect that the critical section related to lock
     * has been completed
     * @param lock
     */
    void informIndividualLockPetersonReleased(ClientGraphPetersonLock lock){
        String prefix = "ecs"; // edge critical section
        String actionVariable = prefix + lock.getBothIdAndIdString();
        setVariable(actionVariable, "0");
    }

    /**
     * release an individual lock in Peterson algorithm
     * @param lock  the Peterson lock
     * @return  true if lock is released
     *          false if some error happens.
     */
    boolean releaseIndividualLockPeterson(ClientGraphPetersonLock lock){
        return releaseIndividualLockPeterson(lock.getNodeFlagVar());
    }

    /**
     * Release an individual lock in Peterson algorithm
     * Says process 5 want to obtain the lock between process 5 and process 6
     * @param myFlagVar     flag56_5
     * @return  true if lock is released
     *          false if some error happens. Actually, it only return true, since all exception will be caught
     *          within the setVariable()
     */
    boolean releaseIndividualLockPeterson(String myFlagVar){
        setVariable(myFlagVar, "0");

//        try{
//            // clear my flag
//            setVariable(myFlagVar, "0");
//        }catch(VoldemortException ve){
//
//            // Duong debug
//            System.out.println(" releaseIndividualLockPeterson: get exception " + ve.getMessage() );
//            System.out.println(" will return false");
//            ve.printStackTrace();
//            System.out.println();
//
//            // should you return false?
//            return false;
//        }

        return true;
    }

    ////////////////////////////////////////////////////////////////////////////////////
    // 4. Static utilitty functions. Practically, these functions could be in any class
    //    But they are placed in this class since they support the client programs
    /////////////////////////////////////////////////////////////////////////////////////

    /**
     * reference
     * http://www.java2s.com/Code/Java/Network-Protocol/FindsalocalnonloopbackIPv4address.htm
     *
     * Finds a local, non-loopback, IPv4 address
     *
     * @return The first non-loopback IPv4 address found, or
     *         <code>null</code> if no such addresses found
     * @throws SocketException
     *            If there was a problem querying the network
     *            interfaces
     */
    public static InetAddress getLocalAddress() throws SocketException {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while( ifaces.hasMoreElements() )
        {
            NetworkInterface iface = ifaces.nextElement();
            Enumeration<InetAddress> addresses = iface.getInetAddresses();

            while( addresses.hasMoreElements() )
            {
                InetAddress addr = addresses.nextElement();
                if( addr instanceof Inet4Address && !addr.isLoopbackAddress() )
                {
                    return addr;
                }
            }
        }

        return null;
    }

    // send message "I AM DONE" to master
    public static void sendFinishSignalToMaster(String masterWaitProgIp, String masterWaitProgPortNumber){
        final String I_AM_DONE_MESSAGE = "I AM DONE";

        System.out.println("Connecting to master node at " + masterWaitProgIp + ":" + masterWaitProgPortNumber);

        try{
            InetSocketAddress masterWaitProgAddr = new InetSocketAddress(InetAddress.getByName(masterWaitProgIp),
                    Integer.valueOf(masterWaitProgPortNumber));

            SocketChannel monitorClient = SocketChannel.open(masterWaitProgAddr);

            byte[] message = new String(I_AM_DONE_MESSAGE).getBytes();

            System.out.println(" Going to send message: \"" + I_AM_DONE_MESSAGE +"\"");
            System.out.println("  Length: " + message.length + " bytes");

            ByteBuffer buffer = ByteBuffer.wrap(message);

            int numberOfBytesWritten = monitorClient.write(buffer);
            buffer.clear();


            System.out.println(numberOfBytesWritten + " bytes are sent");

            monitorClient.close();

        }catch(UnknownHostException e){
            System.out.println(" sendFinishSignalToMaster error: unknown host " + e.getMessage());
            e.printStackTrace();
        }catch(IOException e){
            System.out.println(" sendFinishSignalToMaster error: could not close monitorClient\n");
            e.printStackTrace();
        }

    }

    /**
     * Compute the segment of task assigned to you
     * @param totalNumberOfTasks
     * @param numberOfWorkers
     * @param myWorkerId  assume starting from 0
     * @param myTaskAssignment output. myTaskAssignment[0] is index of first task, myTaskAssignment[1] is index of last task
     *                          assuming starting from 0
     */
    public static void getTaskAssignment(int totalNumberOfTasks, int numberOfWorkers, int myWorkerId, int[] myTaskAssignment){
        int lowerBoundNumberOfTasksPerWorker = (int) Math.floor(totalNumberOfTasks*1.0/numberOfWorkers);
        int numberOfExtraTasks = totalNumberOfTasks - lowerBoundNumberOfTasksPerWorker * numberOfWorkers;

        if(numberOfExtraTasks == 0){
            // even workload
            myTaskAssignment[0] = myWorkerId*lowerBoundNumberOfTasksPerWorker;
            myTaskAssignment[1] = myTaskAssignment[0] + lowerBoundNumberOfTasksPerWorker - 1;
        }else{
            // some lower id worker will work one more task
            if(myWorkerId <= numberOfExtraTasks - 1){
                // you will get one more task
                myTaskAssignment[0] = myWorkerId*(lowerBoundNumberOfTasksPerWorker + 1);
                myTaskAssignment[1] = myTaskAssignment[0] + (lowerBoundNumberOfTasksPerWorker + 1) - 1;
            }else{
                // you will get less task
                myTaskAssignment[0] = numberOfExtraTasks * (lowerBoundNumberOfTasksPerWorker + 1) +
                        (myWorkerId - numberOfExtraTasks) * lowerBoundNumberOfTasksPerWorker;
                myTaskAssignment[1] = myTaskAssignment[0] + lowerBoundNumberOfTasksPerWorker - 1;
            }
        }

    }

    /**
     * Find the minimal integer not present in an array and within range [lowerBound, upperBound]
     * @param arr       array of non-negative integer
     * @param lowerBound
     * @param upperBound
     * @return      positive number if such minimal number exist
     *              if not, extend the range for next available number
     * Example:
     *  arr = [10, 11, 14]
     *      lowerBound = 10 upperBound = 15 => return 12
     *      lowerBound = 14 upperBound = 15 => return 15
     *      lowerBound = 1  upperBound = 2  => return 1
     *      lowerBound = 21 upperBound = 22 => return 21
     *      lowerBound = 11 upperBound = 11 => return 12
     */
    public static int getMinimalValueNotInArray(int[] arr, int lowerBound, int upperBound){
        int[] usedValues = new int[upperBound - lowerBound + 1];
        int arrLength = arr.length;

        for(int i = 0; i < arrLength; i++){
            // mark used values in the range [lowerBound, upperBound]
            if(arr[i] >= lowerBound && arr[i] <= upperBound){
                usedValues[arr[i] - lowerBound] = 1;
            }
        }

        // find the first availabe unused value
        for(int i = lowerBound; i <= upperBound; i++){
            if(usedValues[i - lowerBound] == 0){
                return i;
            }
        }

        // Duong debug
        System.out.println("getMinimalValueNotInArray: error: the range is too small");

        // no such number exists. The reason is the range is too small, extend the range
        return getMinimalValueNotInArray(arr, lowerBound, lowerBound + arrLength + 1);

    }

}
