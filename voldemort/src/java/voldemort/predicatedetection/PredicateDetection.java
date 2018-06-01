package voldemort.predicatedetection;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Maps;
import org.w3c.dom.Element;
import predicatedetectionlib.common.*;
import predicatedetectionlib.common.predicate.*;
import predicatedetectionlib.common.predicate.CandidateMessageContent;
import predicatedetectionlib.versioning.Occurred;
import predicatedetectionlib.common.ByteUtils;
import predicatedetectionlib.versioning.MonitorVectorClock;
import voldemort.serialization.StringSerializer;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.utils.*;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class help detecting whether the local state is met or not
 * Created by duongnn on 5/27/17.
 */
public class PredicateDetection {

    /*********************************************
     ***  SHARED DATA STRUCTURED  ****************
     ********************************************/

    private static final String storeName = "test";
    private static VoldemortConfig config;
    private static StoreRepository storeRepository;

    private static boolean activated;
    private static HVC hvc; // the HVC timestamp maintained at this node

    private static int numberOfMonitors;



    // gets and sets
    public static void setActivated(boolean enableOrNot){
        activated = enableOrNot;
    }
    public static boolean isActivated(){
        return activated;
    }

    public static String getStoreName(){
        return storeName;
    }

    // related to store and data
    public static void setStoreRepository(StoreRepository aStoreRepository){
        storeRepository = aStoreRepository;
    }

    public static StoreRepository getStoreRepository(){
        return storeRepository;
    }

    // since one server has one HVC, but one server has multiple threads running,
    // and the threads could access and modify the HVC, therefore we need
    // synchronized primitive to avoid con-current issues
    public synchronized static void setHvc(HVC anHvc){
        hvc = anHvc;
    }
    public static HVC getHvc(){
        return hvc;
    }

    public static VoldemortConfig getConfig() {
        return config;
    }

    public static void setConfig(VoldemortConfig config) {
        PredicateDetection.config = config;
    }

    public static void setNumberOfMonitors(int num){
        PredicateDetection.numberOfMonitors = num;
    }

    public static int getNumberOfMonitors(){
        return numberOfMonitors;
    }


    /***************************************************
     ***  INNER CLASS FOR PROCESSING HVC  **************
     **************************************************/

    public static class HvcProcessing{
        /**
         * @return return the number of bytes needed to transmit HVC timestamp
         */
        public static int getHvcTimestampSizeInBytes(){
            if(isActivated()) {
                if(getHvc() == null) {
                    // dump HVC
                    return ByteUtils.SIZE_OF_SHORT;
                }
                else{
                    return ByteUtils.SIZE_OF_SHORT + hvc.getSize() * HvcElement.getHvcElementSize();
                }
            }else{
                return 0;
            }
        }

        /**
         * just write a dump HVC timestamp which consist of value 0 (Short)
         * to the outputstream
         * No need to synchronized since this one does not touch real HVC data
         * Also it is done at client side
         * @param outputStream
         */
        private static void writeDumpHvcTimestamp(DataOutputStream outputStream)
                throws IOException {
            if(isActivated()) {
                // insert dump HVC if we are using Predicate Detection
                outputStream.writeShort(0);
            }
        }


        /**
         * write the current HVC timestamp to the output stream
         * @param outputStream
         */
        private synchronized static void writeHvcTimestamp(DataOutputStream outputStream)
                throws IOException{

//            int numOfEntries = hvc.getSize();
//
//            // write the number of entries
//            outputStream.writeShort(numOfEntries);
//
//            // write the timestamp
//            for(int i = 0; i < numOfEntries; i++){
//                outputStream.writeLong(hvc.getHvcTimestamp().elementAt(i).getTimeValue());
//            }

            hvc.writeHvcToStream(outputStream);

        }

        /**
         * read HVC value from stream
         * If not activated, just return null
         * note: null could also be returned if HVC is a dump HVC
         * @param inputStream
         * @return
         */
        public synchronized static HVC readHvcFromStream(DataInputStream inputStream){

            HVC result;

            if(isActivated()){

                result = HVC.readHvcFromStream(inputStream);

            }else{
                result = null;
            }

            return result;
        }

        /**
         * Only for client
         * A wrapper for writeHvcTimestamp and writeDumpHvcTimestamp
         * @param outputStream
         * @throws IOException
         */
        public synchronized  static void writeClientHvcTimestamp(DataOutputStream outputStream)
                throws IOException{
            if(isActivated()){
                if(getHvc() == null){
                    // write dump HVC. This is the case of client when it bootstrap
                    writeDumpHvcTimestamp(outputStream);
                }else{
                    // write normal HVC, when client already know
                    writeHvcTimestamp(outputStream);
                }
            }
        }


        /**
         * Only for client
         * Read HVC from stream and update accordingly
         * @param inputStream
         * @return number of bytes read (i.e. size of HVC in bytes)
         */
        public synchronized static int updateClientHvcFromStream(DataInputStream inputStream){
            if(isActivated()){
                HVC msgHvc = readHvcFromStream(inputStream);

                if(msgHvc == null){
                    // it should not be null. This is an error
                    System.out.println("updateClientHvcFromStream: HVC on message is null");
                    Thread.dumpStack();

                    // null means dump HVC. It has number of entries field = 0
                    return ByteUtils.SIZE_OF_SHORT;
                }else{
                    if(getHvc() == null){
                        // just copy message HVC
                        setHvc(msgHvc);
                    }else {
                        // update client HVC elements to the maximum possible values
                        Vector<HvcElement> maxHvcElement = HVC.getMaxHvcTimestamp(getHvc(), msgHvc);
                        getHvc().setHvcTimestamp(maxHvcElement);

                        // we won't increment local clock since client is not indexed by HVC
                    }

                }

                return ByteUtils.SIZE_OF_SHORT + ByteUtils.SIZE_OF_LONG * msgHvc.getHvcTimestamp().size();
            }else{
                return 0;
            }
        }

        /**
         * Only for server
         * Read HVC from stream and update accordingly
         * @param inputStream
         */
        public synchronized static void updateServerHvcFromStream(DataInputStream inputStream){
            if(PredicateDetection.isActivated()) {
                // reading HVC timestamp from the message
                HVC msgHvc = readHvcFromStream(inputStream);

                if (msgHvc == null) {
                    // we should not get dump HVC anymore
                    System.out.println("updateServerHvcFromStream(): " +
                            "receive dump HVC from client. This should be his first request");

                    // ignore that dump HVC and
                    getHvc().incrementMyLocalClock();

                } else {
                    // update all element to the max
                    Vector<HvcElement> maxHvcElement = HVC.getMaxHvcTimestamp(getHvc(), msgHvc);

                    // Duong debug
                    if(getHvc() == null){
                        System.out.println(" My curent HVC is null, i.e. not initialized yet." +
                                " This should not happens. Give more time for server to start before starting clients!");

                    }


                    getHvc().setHvcTimestamp(maxHvcElement);
                    getHvc().updateActiveSize();

                    // increment your element, which will automatically update element not heard for long.
                    getHvc().incrementMyLocalClock();

                }
            }
        }

        /**
         * Only for server
         * Write server's HVC to stream
         * @param outputStream
         * @throws IOException
         */
        public synchronized static void writeServerHvcTimestamp(DataOutputStream outputStream)
                throws IOException{
            if(PredicateDetection.isActivated()) {
                // write HVC
                writeHvcTimestamp(outputStream);

                // increment HVC local clock
                getHvc().incrementMyLocalClock();
            }
        }

    }


    /*********************************************************
     ****** INNER CLASS FOR LOCAL PREDICATE DETECTION  *******
     *********************************************************/
    public static class LocalPredicateDetection{

        //private static Vector<Predicate> predicateList = null;

        // now predicateList is a mapping of edge string and GraphEdgePredicate
        // we cannot use GraphEdge as key since two GraphEdgePredicate with same content are two keys
        private static ConcurrentHashMap<String, Predicate> predicateList;

        // predicateStatus is a mapping of edge string and the last time this predicate is inactive
        // if timestamp = 0 => active
        //              > 0 => inactive
        private static ConcurrentHashMap<String, Long> predicateStatus;

        private static long predicateExpirationTimeMs;
        private static final int PREDICATE_STATUS_ACTIVE = 0;
        private static final int PREDICATE_STATUS_INACTIVE = 1; // is inactive for a short time
        private static final int PREDICATE_STATUS_EXPIRED = 2;  // has been inactive for a long time, e.g. > 10 * epsilon

        // Duong debug
        private static int maxPredicateListSize = 0;
        private static int maxPredicateStatusSize = 0;

        //private static Table<String, String, SocketChannel> monitorConnectionList; // monitorId, monitorIp:monitorPort, connection
        private static HashMap<String, String> monitorIdAddrMap; // monitorId, monitorIp:monitorPort
        private static HashMap<String, SocketChannel> monitorIdConnectionMap; // monitorId, socketConnection

        //private static Vector<Long> sequenceNumberVector;
        private static HashMap<String, Long> sequenceNumberMap; // mapping between predicate (i.e. edge name like "A_B") and seq. num


        // cache of predicate variables:
        // instead of reading variables related to predicates being monitored from storage
        // we will maintain a cache of those variables, in order to speed up
        // predicate detection processing
        private static Table<String, MonitorVectorClock, String> predVarCache;

        // common lock for predicateList, predicateStatus, predVarCache
        // modification to any of the above data structure should synchronized through this lock
        // private static Object lockPredListStatusVarCache = new Object();

        // state since the last change, i.e. it is older than predVarCache
        // we need this old state since in interval-base predicate detection, the old state will
        // decide whether candidate is sent or not
        private static LocalSnapshotContent currentLocalState;

        private static HVC startPoint;  // the HVC timestamp when the system started being in current state
                                        // this should serve as start point of interval
                                        // startPoint is only updated at put() request
                                        // while hvc is updated at any event/request

        private static long physicalStartTime; // the physical time counterpart of startPoint

        /**
         * entry point of predicate detection
         * @param key
         * @param vClock
         * @param value
         * @return number of candidates sent out
         * This function will update the cache of predicates' variables
         * Then send candidates to monitors if proper
         */
        public synchronized static int checkAndSend(
                ByteArray key,
                VectorClock vClock,
                byte[] value) {

            StringSerializer stringSerializer = new StringSerializer();
            String keyStr = stringSerializer.toObject(key.get());
            String valueStr = stringSerializer.toObject(value);

//            // Duong debug
//            System.out.println("checkAndSend(): keyStr = " + keyStr +
//                    " in hex: " + ByteUtils.toHexString(key.get()) +
//                    "\n, value = " + valueStr + " in hex: " + ByteUtils.toHexString(value));

            // check if the key is related to some predicate
            // since predicates are dynamic (on-the-fly), checking relatedness means checking whether
            // the variable name match some patterns

            //String relatedPredName = Predicate.getRelatedPredicateNameFromKey(keyStr);
            String relatedPredName = Predicate.getRelatedPredicateNameFromKeyEnhanced(keyStr);


            // key is not related to any predicate
            if(relatedPredName == null)
                return 0;



            // convert voldemort vector clock to predicate detection vector clock
            // from now on, in predicate detection module, we only use predicate detection vector clock
            // although the contents of 2 vector clocks are the same
            MonitorVectorClock clock = new MonitorVectorClock(Maps.newTreeMap(vClock.getVersionMap()), vClock.getTimestamp());

            //  update cache
            //    remove old versions
            //    insert if new
            Map<MonitorVectorClock, String> oldVers = getRowFromPredVarCache(keyStr);

            // remove old version
//            for(Iterator<Map.Entry<MonitorVectorClock, String>> it = oldVers.entrySet().iterator(); it.hasNext(); ) {
//                Map.Entry<MonitorVectorClock, String> entry = it.next();
//
//                MonitorVectorClock ver = entry.getKey();
//                Occurred comparison = clock.compare(ver);
//
//                // Duong debug
//                System.out.println(" clock: " + clock.toString());
//                System.out.println(" ver:   " + ver.toString());
//                System.out.println(" clock.compare(ver) = " + comparison);
//
//                if(comparison == Occurred.AFTER) {
//                    it.remove();
//                }
//            }
            synchronized (predVarCache) {
                oldVers.entrySet().removeIf(ver -> clock.compare(ver.getKey()) == Occurred.AFTER);
            }

            // insert if new
            boolean toBeUpdated = true;
            for(Map.Entry<MonitorVectorClock, String> clk : oldVers.entrySet()){
                if(clock.compare(clk.getKey()) == Occurred.BEFORE)
                    toBeUpdated = false;
                break;
            }
            if(toBeUpdated){
                synchronized (predVarCache) {
                    updatePredVarCache(keyStr, clock, valueStr);
                }
            }

            // update predicateList
            if(predicateList.containsKey(relatedPredName)){
                // predicate already exists
                Predicate pred = getPredFromPredicateList(relatedPredName);

                boolean isPredActiveAtServer;
                synchronized (predVarCache){
                    isPredActiveAtServer = pred.isPredicateActiveAtServer(predVarCache);
                }

                if (isPredActiveAtServer) {
                    // mark status as active
                    updatePredicateStatus(relatedPredName, 0L);
                } else {
                    // mark status as inactive
                    updatePredicateStatus(relatedPredName, System.currentTimeMillis());
                }

            }else{
                // predicate not exists

                // calculating the id of monitor and monitor address information
                // note that number of monitor is equal to number of server
                int monitorId = Math.abs(relatedPredName.hashCode() % numberOfMonitors);
                String[] monitorAddr = monitorIdAddrMap.get(String.valueOf(monitorId)).split(":");

                // add new predicate to predicateList
                Predicate newPred = Predicate.generatePredicateFromNameEnhanced(relatedPredName, monitorAddr[0], monitorAddr[1]);
                updatePredicateList(relatedPredName, newPred);

                // mark status accordingly
                boolean isNewPredActiveAtServer;
                synchronized (predVarCache){
                    isNewPredActiveAtServer = newPred.isPredicateActiveAtServer(predVarCache);
                }

                if(isNewPredActiveAtServer)
                    updatePredicateStatus(relatedPredName, 0L);
                else
                    updatePredicateStatus(relatedPredName, System.currentTimeMillis());
            }

            // update sequenceNumberMap
            if(!sequenceNumberMap.containsKey(relatedPredName)){
                sequenceNumberMap.put(relatedPredName, 0L);
            }

            // for each predicate
            //      send candidate if proper
            // note that the candidates sent to monitors will have different sequence number,
            // but will have same predicateName and local snapshot content
            // The HVC interval is generally the same. However, in case of expire predicate, we
            // send special interval where the endpoint is infinity

            // prepare Hvc interval and local state for message
            HvcInterval outgoingHvcInterval = new HvcInterval(getPhysicalStartTime(), getStartPoint(), getHvc());
            LocalSnapshotContent oldState = getCurrentLocalState();

            int numberOfCandidatesSent = 0;

            // recording size of hashmap
            if(maxPredicateListSize < predicateList.size()){
                maxPredicateListSize = predicateList.size();
            }
            if(maxPredicateStatusSize < predicateStatus.size()){
                maxPredicateStatusSize = predicateStatus.size();
            }

            //for(Map.Entry<String, Predicate> entry : predicateList.entrySet()){
            for(Iterator<Map.Entry<String, Predicate>> it = predicateList.entrySet().iterator(); it.hasNext(); ){
                Map.Entry<String, Predicate> entry = it.next();

                String predicateName = entry.getKey();
                Predicate pred = entry.getValue();
                int predStatus = getPredicateStatus(pred);

                // we only need to transmit part of the cache (oldState) that is related to the
                // current predicate under consideration, i.e. only variables that appear
                // in the predicate pred
                LocalSnapshotContent projectedOldState = oldState.projectOnPredicate(pred);

                switch (pred.getType()) {
                    case LINEAR:
                        switch(predStatus){
                            case PREDICATE_STATUS_ACTIVE:
                            case PREDICATE_STATUS_INACTIVE:
                                // send only when local pred is true
                                // if (pred.evaluate(oldState) == true) {
                                if (pred.evaluate(projectedOldState) == true) {
                                    // prepare localsnapshot content
                                    long seqNumber = sequenceNumberMap.get(predicateName) + 1;
                                    sequenceNumberMap.put(predicateName, seqNumber);

                                    LocalSnapshot outgoingLocalSnapshot =
                                            new LocalSnapshot(outgoingHvcInterval, seqNumber, projectedOldState);
                                    CandidateMessageContent outgoingCandidateMessageContent =
                                            new CandidateMessageContent(predicateName, outgoingLocalSnapshot);

                                    try {
                                        int monitorId = Math.abs(predicateName.hashCode() % numberOfMonitors);

                                        sendCandidateToMonitor(outgoingCandidateMessageContent, monitorId);
                                        numberOfCandidatesSent ++;

                                    }catch(Exception e){
                                        log("checkAndSend error: " + e.getMessage());
                                        e.printStackTrace();
                                    }
                                }
                                break;

                            case PREDICATE_STATUS_EXPIRED:
                                // send special candidate
                                if(pred.evaluate(projectedOldState) == true){
                                    System.out.println("Strange error: expired linear predicate should not be true");
                                }else{
                                    // prepare localsnapshot content
                                    long seqNumber = sequenceNumberMap.get(predicateName) + 1;
                                    sequenceNumberMap.put(predicateName, seqNumber);

                                    HVC infinityHvc = HVC.getInfinityHVC(getHvc().getSize(), getHvc().getEpsilon(), getHvc().getNodeId());
                                    outgoingHvcInterval.setEndPoint(infinityHvc);
                                    LocalSnapshot outgoingLocalSnapshot =
                                            new LocalSnapshot(outgoingHvcInterval, seqNumber, projectedOldState);
                                    CandidateMessageContent outgoingCandidateMessageContent =
                                            new CandidateMessageContent(predicateName, outgoingLocalSnapshot);

                                    try {
                                        int monitorId = Math.abs(predicateName.hashCode() % numberOfMonitors);

                                        sendCandidateToMonitor(outgoingCandidateMessageContent, monitorId);
                                        numberOfCandidatesSent ++;

                                    }catch(Exception e){
                                        log("checkAndSend error: " + e.getMessage());
                                        e.printStackTrace();
                                    }
                                }

                                break;
                        } // switch (predStatus)

                        break;

                    case SEMILINEAR:

                        // send any way, monitor has to examine its content.

                        // prepare localsnapshot content
                        long seqNumber = sequenceNumberMap.get(predicateName) + 1;
                        sequenceNumberMap.put(predicateName, seqNumber);

                        // However, the end point of HVC interval depends on the status of predicate
                        switch(predStatus){
                            case PREDICATE_STATUS_ACTIVE:
                            case PREDICATE_STATUS_INACTIVE:
                                // send candidate normally, no change in HVC interval
                                break;
                            case PREDICATE_STATUS_EXPIRED:
                                // send special candidate with HVC interval endpoint = infinity
                                HVC infinityHvc = HVC.getInfinityHVC(getHvc().getSize(), getHvc().getEpsilon(), getHvc().getNodeId());
                                outgoingHvcInterval.setEndPoint(infinityHvc);

                                break;
                        }

                        LocalSnapshot outgoingLocalSnapshot =
                                new LocalSnapshot(outgoingHvcInterval, seqNumber, projectedOldState);
                        CandidateMessageContent outgoingCandidateMessageContent =
                                new CandidateMessageContent(predicateName, outgoingLocalSnapshot);

                        try {
                            int monitorId = Math.abs(predicateName.hashCode() % numberOfMonitors);
                            sendCandidateToMonitor(outgoingCandidateMessageContent, monitorId);
                            numberOfCandidatesSent ++;
                        }catch(Exception e){
                            log("checkAndSend error: " + e.getMessage());
                            e.printStackTrace();
                        }

                        break;
                }

                // if predicate has expired, clean up data structure for it
                if(predStatus == PREDICATE_STATUS_EXPIRED){
                    cleanUpExpiredPredicate(pred);
                }
            }

            // update state to the new one
            updateLocalStateContentFromCache(predVarCache);

            // Duong debug
//            log("  *** " + numberOfCandidatesSent + " candidates sent");

            return numberOfCandidatesSent;

        }


        // synchronized functions for modifying predicateStatus
        static void updatePredicateStatus(String predName, long status) {
            synchronized (predicateStatus) {
                predicateStatus.put(predName, status);
            }
        }

        static int getPredicateStatus(Predicate pred){
            long inactiveTime;
            synchronized (predicateStatus) {
                inactiveTime = predicateStatus.get(pred.getPredicateName());
            }

            if (inactiveTime == 0) {
                // still active
                return PREDICATE_STATUS_ACTIVE;
            } else {
                // inactive
                if (System.currentTimeMillis() - inactiveTime < predicateExpirationTimeMs) {
                    // inactive but not exprired
                    return PREDICATE_STATUS_INACTIVE;
                } else {
                    // inactive and expired
                    return PREDICATE_STATUS_EXPIRED;
                }
            }
        }

        static void removePredFromPredicateStatus(String predName) {
            synchronized (predicateStatus) {
                predicateStatus.remove(predName);
            }
        }

        // synchronized functions for modifying predicateList
        static void updatePredicateList(String predName, Predicate pred) {
            synchronized (predicateList) {
                predicateList.put(predName, pred);
            }
        }
        static Predicate getPredFromPredicateList(String predName) {
            synchronized (predicateList) {
                return predicateList.get(predName);
            }
        }
        static void removePredFromPredicateList(String predName) {
            synchronized (predicateList) {
                predicateList.remove(predName);
            }
        }

        // synchronized functions for modifying predVarCache
        static void updatePredVarCache(String keyStr, MonitorVectorClock clock, String valueStr) {
            synchronized (predVarCache) {
                predVarCache.put(keyStr, clock, valueStr);
            }
        }

        static Map<MonitorVectorClock, String> getRowFromPredVarCache(String variableName) {
            synchronized (predVarCache) {
                return predVarCache.row(variableName);
            }
        }

        static void removeEntryPredVarCache(String variableName, MonitorVectorClock ver) {
            synchronized (predVarCache) {
                predVarCache.remove(variableName, ver);
            }
        }

        static void cleanUpExpiredPredicate(Predicate pred){
            String predicateName = pred.getPredicateName();

            // remove the predicate from predicateList
            removePredFromPredicateList(predicateName);

            // remove predicate from predicateStatus
            removePredFromPredicateStatus(predicateName);

            // Remove associated variable from predVarCache only if predicate does not share
            // variables with other predicates
            // Example of such is GraphEdgePredicate
            // If predicate shares variables with others, removing the variables will affect other predicates
            if(!doesPredicateShareVariables(pred)){
                // get list of variable names associated with this GraphEdgePredicate
                HashMap<String, String> variableList = pred.getVariableList();

                synchronized (predVarCache) {
                    for (String variableName : variableList.keySet()) {
                        Map<MonitorVectorClock, String> verAndValMap = getRowFromPredVarCache(variableName);

                        // for each version of this variable, remove it from predVarCache
                        for (Map.Entry<MonitorVectorClock, String> verAndVal : verAndValMap.entrySet()) {
                            MonitorVectorClock ver = verAndVal.getKey();
                            removeEntryPredVarCache(variableName, ver);
                        }
                    }
                }

            }
        } // end of clean up data structure



        static boolean doesPredicateShareVariables(Predicate pred){
            if(pred instanceof GraphEdgePredicate)
                return false;

            // for other instances of predicate
            // TBD

            // for safe, assume it shares
            return true;
        }


        public static synchronized LocalSnapshotContent getCurrentLocalState(){
            return currentLocalState;
        }

        // this function should be called within a block that checks isActivated
        public static synchronized void setCurrentLocalState(LocalSnapshotContent localState){
            currentLocalState = localState;
        }

        public static synchronized HVC getStartPoint(){
            return startPoint;
        }

        private static synchronized void setStartPoint(HVC point){
            startPoint = new HVC(point);
        }

        public static synchronized long getPhysicalStartTime(){
            return physicalStartTime;
        }

        public static synchronized void setPhysicalStartTime(long someTime){
            physicalStartTime = someTime;
        }

        public static synchronized void setPredicateExpirationTimeMs(long expirationTimeMs){
            predicateExpirationTimeMs = expirationTimeMs;
        }

        // Advance startPoint to value of current hvc.
        // Used to advance the interval after change in local state
        public static synchronized void advanceStartPointToCurrentHvc(){
            if(PredicateDetection.isActivated()) {
                setStartPoint(hvc);
                setPhysicalStartTime(System.currentTimeMillis());
            }
        }

//        /**
//         * Initialize:
//         *  Predicate list
//         *  cache of variables predVarCache
//         *  vector of sequence numbers
//         * @param predicateElementList
//         */
//        public static void initPredicateList(Vector<Element> predicateElementList){
//            if(isActivated()) {
//
//                // Duong debug
//                System.out.println("\n\n\n -----------------");
//                System.out.println("  Initializing predVarCache");
//
//                Store<ByteArray, byte[], byte[]> store = PredicateDetection
//                        .getStoreRepository()
//                        .getLocalStore(PredicateDetection.getStoreName());
//                StringSerializer stringSerializer = new StringSerializer();
//
//
//                int numberOfPredicates = predicateElementList.size();
//                predicateList = new Vector<Predicate>(numberOfPredicates);
//
//                predVarCache = HashBasedTable.create();
//
//                // scan over the list of predicates, add them to predicateList
//                // and add predicate's variables to the cache
//                for (int i = 0; i < predicateElementList.size(); i++) {
//                    Predicate pred = XmlUtils.elementToPredicate(predicateElementList.elementAt(i));
//                    predicateList.addElement(pred);
//
//                    // add predicate's variables to cache
//                    for (Map.Entry<String, String> entry : pred.getVariableList().entrySet()) {
//
//                        // Duong debug
//                        System.out.println(" considering key: " + entry.getKey());
//
//                        // add variable to the cache if the cache has not contained this variable yet
//                        if (!predVarCache.containsRow(entry.getKey())) {
//
//                            // Duong debug
//                            System.out.println(" key " + entry.getKey() + " is not in cache yet. Going to insert");
//                            int versionCount = 0;
//
//                            // obtain versions and values
//                            ByteArray keyInByteArray = new ByteArray(entry.getKey().getBytes());
//                            List<Versioned<byte[]>> versionedList = store.get(keyInByteArray, null);
//
//                            // add to cache
//                            if (versionedList.size() == 0) {
//
//                                // Duong debug
//                                System.out.println("   key does not exist in database. Inserting dump entry");
//
//                                // even the storage does not have this key
//                                // we need to create a "dump" entry for this key
//                                VectorClock vClock = new VectorClock();
//                                MonitorVectorClock dumpClock =
//                                        new MonitorVectorClock(Maps.newTreeMap(vClock.getVersionMap()), vClock.getTimestamp());
//
//                                // make sure value is not the desired one so that the predicate would not be true
//                                String dumpValue = "i5n!G" + entry.getValue();
//
//                                // Duong debug
//                                System.out.println("      the size of MonitorVectorClock in dump entry is " + dumpClock.sizeInBytes());
//
//                                predVarCache.put(entry.getKey(), dumpClock, dumpValue);
//
//                            } else {
//
//                                // Duong debug
//                                System.out.println("   database has the key. Inserting existing value");
//
//                                // the key already exists in storage
//                                for (Versioned<byte[]> ver : versionedList) {
//
//                                    String valueStr = stringSerializer.toObject(ver.getValue());
//                                    VectorClock vClock = (VectorClock) ver.getVersion();
//                                    MonitorVectorClock clock =
//                                            new MonitorVectorClock(Maps.newTreeMap(vClock.getVersionMap()), vClock.getTimestamp());
//
//                                    String retVal = predVarCache.put(entry.getKey(), clock, valueStr);
//
//                                }
//                            }
//
//                        } else {
//                            // Duong debug
//                            System.out.println(" key " + entry.getKey() + " is already in cache. No need to insert");
//                        }
//                    }
//                }
//
//
//                // update currentLocalState to the cache
//                updateLocalStateContentFromCache(predVarCache);
//
//                // Duong debug
//                System.out.println(" initial cache: ");
//                System.out.println(getCurrentLocalState().toString(0));
//
//                // init sequence number vector
//                sequenceNumberVector = new Vector<>(numberOfPredicates);
//                for(int i = 0; i < numberOfPredicates; i++){
//                    sequenceNumberVector.addElement(0L);
//                }
//
//                // init permanent connections to monitors
//                monitorConnectionList = HashBasedTable.create();
//
//                synchronized (predicateList) {
//                    for (Predicate pred : predicateList) {
//                        try {
//                            String monitorIpAddr = pred.getMonitorIpAddr();
//                            String monitorPortNumber = pred.getMonitorPortNumber();
//
//                            // open connection if it is new
//                            if (!monitorConnectionList.contains(monitorIpAddr, monitorPortNumber)) {
//                                InetSocketAddress monitorAddr = new InetSocketAddress(InetAddress.getByName(monitorIpAddr),
//                                        Integer.valueOf(monitorPortNumber));
//                                SocketChannel monitorClient = SocketChannel.open(monitorAddr);
//
//                                // keep this connection alive
//                                monitorClient.socket().setKeepAlive(true);
//
//                                monitorConnectionList.put(monitorIpAddr, monitorPortNumber, monitorClient);
//                            } else {
//                                // should not be in this case
//                                // something wrong
//                                log("initPredicateList() error: connection already exists");
//                                log("  predicate: \n" + pred.toString(2) + "\n");
//                            }
//                        } catch (Exception e) {
//                            log("initPredicateList() error: could not open connection to monitor for predicate");
//                            log(pred.toString(2));
//                            log(e.getMessage());
//                        }
//                    }
//                }
//            }
//        }


        public static void clearPredicateList(){
            if (isActivated()) {

                System.out.println("maxPredicateListSize =     " + maxPredicateListSize);
                System.out.println("maxPredicateStatusSize =   " + maxPredicateStatusSize);
                System.out.println("sequenceNumberMap.size() = " + sequenceNumberMap.size());

                LocalPredicateDetection.predicateList.clear();
                LocalPredicateDetection.predicateStatus.clear();
                sequenceNumberMap.clear();

                // closing connections to monitors
                for(Map.Entry<String, SocketChannel> monitorId_monitorConnection : monitorIdConnectionMap.entrySet()){
                    SocketChannel monitorClient = monitorId_monitorConnection.getValue();

                    try{
                        if(monitorClient.isOpen()){
                            monitorClient.close();
                        }
                    }catch(Exception e){
                        log("clearPredicateList() error: could not close channel " + monitorClient.toString());
                        log(e.getMessage());
                    }
                }

                // clear connections to monitors
                monitorIdConnectionMap.clear();
                monitorIdAddrMap.clear();
            }
        }

        /**
         * Initialize data structures for predicate detection
         *      Predicate list
         *      Predicate status
         *      cache of variables predVarCache
         *      vector of sequence numbers
         *  @param monitorElementList vector of monitor elements
         *  @param staticPredicateElementList list of static predicates
         */
        public static void initPredicateDetection(Vector<Element> staticPredicateElementList, Vector<Element> monitorElementList){
            if(isActivated()) {

                // Duong debug
                System.out.println("\n\n\n -----------------");
                System.out.println("  Initializing predVarCache:");

                Store<ByteArray, byte[], byte[]> store = PredicateDetection
                        .getStoreRepository()
                        .getLocalStore(PredicateDetection.getStoreName());
                StringSerializer stringSerializer = new StringSerializer();

                LocalPredicateDetection.predicateList = new ConcurrentHashMap<>();
                LocalPredicateDetection.predicateStatus = new ConcurrentHashMap<>();
                LocalPredicateDetection.predVarCache = HashBasedTable.create();

                // init sequence number map
                // for graph with 100K nodes, there are 300K edges, so each server 100K edges
                sequenceNumberMap = new HashMap<>(100000);

                // adding static predicates
                if(staticPredicateElementList != null) {
                    // for each predicate in the list of static predicates
                    //  add them to predicateList
                    //              predicateStatus
                    //              sequenceNumberMap
                    //  and add predicate's variables to the cache
                    for (int i = 0; i < staticPredicateElementList.size(); i++) {
                        Predicate pred = XmlUtils.elementToPredicate(staticPredicateElementList.elementAt(i));

                        updatePredicateList(pred.getPredicateName(), pred);

                        // assume they are active at first
                        // if they are really inactive, status will be updated later
                        updatePredicateStatus(pred.getPredicateName(), 0L);

                        if(!sequenceNumberMap.containsKey(pred.getPredicateName())){
                            sequenceNumberMap.put(pred.getPredicateName(), 0L);
                        }

                        // add predicate's variables to cache
                        for (Map.Entry<String, String> entry : pred.getVariableList().entrySet()) {

                            // Duong debug
                            System.out.println(" considering key: " + entry.getKey());

                            // add variable to the cache if the cache has not contained this variable yet
                            if (!predVarCache.containsRow(entry.getKey())) {


                                // Duong debug
                                System.out.println(" key " + entry.getKey() + " is not in cache yet. Going to insert");
                                int versionCount = 0;

                                // obtain versions and values
                                ByteArray keyInByteArray = new ByteArray(entry.getKey().getBytes());
                                List<Versioned<byte[]>> versionedList = store.get(keyInByteArray, null);

                                // add to cache
                                if (versionedList.size() == 0) {

                                    // Duong debug
                                    System.out.println("   key does not exist in database. Inserting dump entry");

                                    // even the storage does not have this key
                                    // we need to create a "dump" entry for this key
                                    VectorClock vClock = new VectorClock();
                                    MonitorVectorClock dumpClock =
                                            new MonitorVectorClock(Maps.newTreeMap(vClock.getVersionMap()), vClock.getTimestamp());

                                    // make sure value is not the desired one so that the predicate would not be true
                                    String dumpValue = "i5n!G" + entry.getValue();

//                                    // Duong debug
//                                    System.out.println("      the size of MonitorVectorClock in dump entry is " + dumpClock.sizeInBytes());

                                    updatePredVarCache(entry.getKey(), dumpClock, dumpValue);

                                } else {

                                    // Duong debug
                                    System.out.println("   database has the key. Inserting existing value");

                                    // the key already exists in storage
                                    for (Versioned<byte[]> ver : versionedList) {

                                        String valueStr = stringSerializer.toObject(ver.getValue());
                                        VectorClock vClock = (VectorClock) ver.getVersion();
                                        MonitorVectorClock clock =
                                                new MonitorVectorClock(Maps.newTreeMap(vClock.getVersionMap()), vClock.getTimestamp());

                                        updatePredVarCache(entry.getKey(), clock, valueStr);

                                    }
                                }

                            } else {
                                // Duong debug
                                System.out.println(" key " + entry.getKey() + " is already in cache. No need to insert");
                            }
                        }
                    }
                }

                // update currentLocalState to the cache
                updateLocalStateContentFromCache(LocalPredicateDetection.predVarCache);

                // Duong debug
                System.out.println(" initial cache: ");
                System.out.println(getCurrentLocalState().toString(0));

                // Init permanent connections to monitors
                // Note that we have one monitor on the same machine as one server
                // i.e. the number of monitors = number of servers
                monitorIdAddrMap = new HashMap<>();
                monitorIdConnectionMap = new HashMap<>();

                for(int i = 0; i < monitorElementList.size(); i++){
                    Element monitorElement = monitorElementList.elementAt(i);

                    String monitorId = monitorElement.getElementsByTagName("id").item(0).getTextContent();
                    Element monitorAddrElement = (Element) monitorElement.getElementsByTagName("monitorAddr").item(0);
                    String monitorIpAddr = monitorAddrElement.getElementsByTagName("ip").item(0).getTextContent();
                    String monitorPortNumber = monitorAddrElement.getElementsByTagName("port").item(0).getTextContent();

                    try {
                        // open connection if it is new
                        if(!monitorIdAddrMap.containsKey(monitorId)){
                            InetSocketAddress monitorAddr = new InetSocketAddress(InetAddress.getByName(monitorIpAddr),
                                                                                Integer.valueOf(monitorPortNumber));
                            SocketChannel monitorClient = SocketChannel.open(monitorAddr);

                            // keep this connection alive
                            monitorClient.socket().setKeepAlive(true);

                            monitorIdAddrMap.put(monitorId, monitorIpAddr + ":" + monitorPortNumber);
                            monitorIdConnectionMap.put(monitorId, monitorClient);
                        }
                    }catch(Exception e) {
                        log("initPredicateDetection() error: could not open connection to monitor");
                        log(" monitorId = " + monitorId + " monitorAddr = " + monitorIpAddr + ":" + monitorPortNumber);
                        log(e.getMessage());
                    }
                }
            }
        }

        // make current local state to catch up with cache content
        public synchronized static void updateLocalStateContentFromCache(
                Table<String, MonitorVectorClock, String> cache){
            setCurrentLocalState(new LocalSnapshotContent(cache));
        }

        /**
         * send a candidate/local snapshot to a monitor at address provided
         * @param candidateMessageContent
         * @param monitorId
         * @throws IOException
         * @throws InterruptedException
         */
        private synchronized static void sendCandidateToMonitor(
                CandidateMessageContent candidateMessageContent,
                int monitorId)
                throws IOException, InterruptedException {

            // retrieve the existing connection to monitor
            String monitorIdStr = String.valueOf(monitorId);
            SocketChannel monitorClient = monitorIdConnectionMap.get(monitorIdStr);

            // it is possible the connection is closed by monitor, then we need to reopen it
            if(monitorClient.socket().isClosed()){

                log(" sendCandidateToMonitor: connection is already closed. Reopening it");

                // close
                monitorClient.close();

                // remove from

                // and reopen it
                String[] monitorAddr = monitorIdAddrMap.get(monitorIdStr).split(":");
                InetSocketAddress monitorAddrSocket = new InetSocketAddress(InetAddress.getByName(monitorAddr[0]),
                                                                        Integer.valueOf(monitorAddr[1]));
//                log("    Reconnecting to Monitor on at " + monitorAddrSocket +
//                        " for sequence number " + localSnapshot.getSequenceNumber() + " ...");

                monitorClient = SocketChannel.open(monitorAddrSocket);
                monitorIdConnectionMap.put(monitorIdStr, monitorClient);

            }else{
//                log(" sendCandidateToMonitor: connection is still active");
            }

            int[] cmcLength = new int[1];
            byte[] message = candidateMessageContent.toBytes(cmcLength);

//            log(" Server" + config.getNodeId() + ": sending "
//                    + cmcLength[0] + " bytes to monitor " + monitorIdStr
//                    + " for predicate " + candidateMessageContent.getPredicateName());
//            log(candidateMessageContent.toString(0));

            ByteBuffer buffer = ByteBuffer.wrap(message);

            int numberOfBytesWritten;

            // we should have one server thread use one connection at a time
            synchronized (monitorClient) {
                numberOfBytesWritten = monitorClient.write(buffer);
            }

//            log("      " + numberOfBytesWritten + " bytes are sent");

        }
    }


    /***************************************
     ***** MISCELLANEOUS  ******************
     ***************************************/

    private static void log(String str) {
        System.out.println(str);
    }


}
