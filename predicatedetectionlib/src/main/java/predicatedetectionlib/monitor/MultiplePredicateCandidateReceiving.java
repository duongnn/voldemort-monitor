package predicatedetectionlib.monitor;

import predicatedetectionlib.common.*;
import predicatedetectionlib.common.predicate.CandidateMessageContent;
import predicatedetectionlib.common.predicate.Predicate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static predicatedetectionlib.common.CommonUtils.*;
import static predicatedetectionlib.monitor.Monitor.getEpsilon;
import static predicatedetectionlib.monitor.MultiplePredicateMonitor.markPredicateStatusActive;
import static predicatedetectionlib.monitor.MultiplePredicateMonitor.sharedLockForReceiverAndProcessor;

/**
 * Created by duongnn on 3/27/18.
 */
public class MultiplePredicateCandidateReceiving implements Runnable {

    ConcurrentHashMap<String, Vector<InternalBuffer>> predicateInternalBufferMap;
    ConcurrentHashMap<String, Vector<LinkedBlockingQueue<LocalSnapshot>>> predicateCandidateQueueArrayMap;
    ConcurrentHashMap<String, GlobalSnapshot> predicateGlobalSnapshotMap;
    ConcurrentHashMap<String, Predicate> monitorPredicateList;
    ConcurrentHashMap<String, Long> monitorPredicateStatus;

    // this map a socket channel to a channel bytebuffer
    // data stream will be read from channel into associated bytebuffer first,
    // then packets will be extracted from byte buffer
    ConcurrentHashMap<String, ByteBuffer> monitorChannelBufferMap;

    ConcurrentLinkedQueue<CandidateMessageContent> monitorExtractedCmcQueue;

    // some constant for estimating space needed for buffer
    final int RESERVED_SPACE = 100;
    final int MAX_BUFFER_SIZE = ByteUtils.SIZE_OF_CHAR * 20 // estimate upper bound for size of predicateName
            + ByteUtils.SIZE_OF_INT * 3
            + ByteUtils.SIZE_OF_LONG
            + ByteUtils.SIZE_OF_LONG * LocalSnapshot.MAX_NUMBER_OF_PROCESSES
            + ByteUtils.SIZE_OF_INT * LocalSnapshotContent.MAX_NUMBER_OF_ROWS
            + LocalSnapshotContent.MAX_NUMBER_OF_ROWS * LocalSnapshotContent.MAX_COLUMN_STRING_LENGTH * ByteUtils.SIZE_OF_CHAR
            + RESERVED_SPACE;

    MultiplePredicateCandidateReceiving(
            ConcurrentHashMap<String, Vector<InternalBuffer>> predicateInternalBufferMap,
            ConcurrentHashMap<String, Vector<LinkedBlockingQueue<LocalSnapshot>>> predicateCandidateQueueArrayMap,
            ConcurrentHashMap<String, GlobalSnapshot> predicateGlobalSnapshotMap,
            ConcurrentHashMap<String, Predicate> monitorPredicateList,
            ConcurrentHashMap<String, Long> predicateStatus){

        this.predicateInternalBufferMap = predicateInternalBufferMap;
        this.predicateCandidateQueueArrayMap = predicateCandidateQueueArrayMap;
        this.predicateGlobalSnapshotMap = predicateGlobalSnapshotMap;
        this.monitorPredicateList = monitorPredicateList;
        this.monitorPredicateStatus = predicateStatus;

        monitorChannelBufferMap = new ConcurrentHashMap<>();
        monitorExtractedCmcQueue = new ConcurrentLinkedQueue<>();

    }

    public void run(){
        try {
            // Selector: multiplexer of SelectableChannel objects
            Selector selector = Selector.open(); // selector is open here

            // ServerSocketChannel: selectable channel for stream-oriented listening sockets
            ServerSocketChannel monitorSocket = ServerSocketChannel.open();
            InetSocketAddress monitorAddr = new InetSocketAddress(getLocalNonLoopbackAddress(), MultiplePredicateMonitor.getMonitorPortNumber());

            // Binds the channel's socket to a local address and configures the socket to listen for connections
            monitorSocket.bind(monitorAddr);

            // Adjusts this channel's blocking mode to non-blocking
            monitorSocket.configureBlocking(false);

            // set timeout so that it won't close too early
            // this is important so that the connection between server and monitor is persistent
            monitorSocket.socket().setSoTimeout(0);

            int ops = monitorSocket.validOps();
            SelectionKey selectKey = monitorSocket.register(selector, ops, null);

            CommonUtils.log("\n");
            CommonUtils.log("Candidate receiving process:");
            CommonUtils.log("          numberOfServer = " + MultiplePredicateMonitor.getNumberOfServers());
            CommonUtils.log("                 epsilon = " + getEpsilon());
            CommonUtils.log(" waiting for msg address = " + monitorAddr.getAddress() + ":" + MultiplePredicateMonitor.getMonitorPortNumber()); //SinglePredicateMonitor.getMonitorPortNumber());
            CommonUtils.log("\n");

            // Keep server running until it receive SIGINT signal

            while (!MultiplePredicateMonitor.stopThreadNow) {

                // Selects a set of keys whose corresponding channels are ready for I/O operations
                selector.select();

                // token representing the registration of a SelectableChannel with a Selector
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey currentKey = keyIterator.next();

                    // Tests whether this key's channel is ready to accept a new socket connection
                    if (currentKey.isAcceptable()) {
                        SocketChannel voldServer = monitorSocket.accept();

                        // try to make socket not close
                        voldServer.socket().setSoTimeout(0);

                        // Adjusts this channel's blocking mode to false
                        voldServer.configureBlocking(false);

                        // Operation-set bit for read operations
                        voldServer.register(selector, SelectionKey.OP_READ);

                        //CommonUtils.log("Connection Accepted: " + voldServer.getLocalAddress() + "\n");

                        // create a byte buffer and associate with this channel if this is new.
                        // Do not override old map since byte buffer in old mapping could contain data
                        String voldServerStr = getStringRepresentSocketChannel(voldServer);
                        if(! monitorChannelBufferMap.containsKey(voldServerStr)) {
                            ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_BUFFER_SIZE);
                            monitorChannelBufferMap.put(voldServerStr, byteBuffer);
                        }

                        // Tests whether this key's channel is ready for reading
                    } else if (currentKey.isReadable()) {

                        SocketChannel voldServer = (SocketChannel) currentKey.channel();

                        // obtain associated byte buffer
                        ByteBuffer byteBuffer = monitorChannelBufferMap.get(getStringRepresentSocketChannel(voldServer));

                        // read data stream from channel into buffer
                        // remember to change state of buffer after finish
                        readDataFromChannelIntoBuffer(voldServer, byteBuffer);

                        // extract packets from byte buffer
                        // remember to change state of buffer after finish
                        extractCmcPacketsFromBufferIntoQueue(byteBuffer);

                        // pre-processing the extraced CandidateMessageContent packets
                        // i.e. put them into appropriate internal buffer
                        preprocessExtractedCmcPackets();

                    }

                    keyIterator.remove();
                }
            }

            // Duong debug
            System.out.println("MultiplePredicateCandidateReceiving thread ended");

        }catch(Exception e){
            CommonUtils.log(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * Read data from socket channel into a buffer
     * The reason for this step is data from channel are stream of bytes which may correspond
     * to a single packet, partial packet, or multiple packets.
     * So we need to put them into buffer and extract packets out of buffer
     * @param socketChannel
     * @param byteBuffer
     */
    private void readDataFromChannelIntoBuffer(SocketChannel socketChannel, ByteBuffer byteBuffer){
        // when function is called, byteBuffer may:
        //    just been allocated
        //    has finish extracting (i.e. reading from buffer to packet queue)
        // so it should be ready for being written
        try {
            socketChannel.read(byteBuffer);
        }catch (IOException ioe){
            System.out.println("readDataFromChannelIntoBuffer: ERROR: Cannot read data from channel " +
                    getStringRepresentSocketChannel(socketChannel) +
                    " into buffer");
            System.out.println(ioe.getMessage());
        }

        // not sure if flip is appropriate here
        byteBuffer.flip();
    }

    /**
     * Extract as many packet out of buffer as possible
     * Then compact the buffer so that we can read new data into the buffer
     * When the function begins, position should be 0
     * When the function finishes, position should be the last byte available
     * @param byteBuffer
     */
    private void extractCmcPacketsFromBufferIntoQueue(ByteBuffer byteBuffer){
        // when this function is called, byte buffer is ready for being read
        // i.e. position should point to the last byte written
        CandidateMessageContent cmc;
        while((cmc = extractCandidateMessageContentFromByteBuffer(byteBuffer)) != null){
            // put cmc into queue
            monitorExtractedCmcQueue.add(cmc);
        }

        // compact buffer for further reading new data
        byteBuffer.compact();

    }

    /**
     * Extract a CandidateMessageContent packet from buffer if possible
     * @param byteBuffer
     * @return a CandidateMessageContent if available, position of buffer should be advanced
     *         null if data is not complete for a CandidateMessageContent, position should be unchanged
     */
    private CandidateMessageContent extractCandidateMessageContentFromByteBuffer(ByteBuffer byteBuffer){
        // mark current position
        byteBuffer.mark();

        // probe if a CandidateMessageContent is available
        if(! CandidateMessageContent.quickProbeByteBuffer(byteBuffer)){
            // restore value of position
            byteBuffer.reset();

            // return null indicating data has not fully arrived
            return null;
        }

        // restore value position for extraction
        byteBuffer.reset();

        // extract CandidateMessageContent from byte buffer
        int remainingBytes = byteBuffer.remaining();
        int positionBeforeExtract = byteBuffer.position();
        CandidateMessageContent extractedCmc = new CandidateMessageContent();
        int numOfBytes = extractedCmc.fromBytes(byteBuffer.array(), positionBeforeExtract);

        // advance position to new value
        byteBuffer.position(positionBeforeExtract + numOfBytes);

//        CommonUtils.log("\n extractCandidateMessageContentFromByteBuffer: extract CMC from " + numOfBytes +
//                " out of " + remainingBytes + " bytes." +
//                " Old pos = " + positionBeforeExtract + " new pos = " + byteBuffer.position());

        return extractedCmc;

    }

    private void preprocessExtractedCmcPackets(){
        // while the queue is not empty
        //    retrieve the head
        //    process it
        while(! monitorExtractedCmcQueue.isEmpty()){
            CandidateMessageContent cmc = monitorExtractedCmcQueue.poll();

            // extract predicate name
            String predicateName = cmc.getPredicateName();

            // extract local snapshot
            LocalSnapshot localSnapshot = cmc.getLocalSnapshot();
            localSnapshot.setReceivedTime(System.currentTimeMillis());
            int processId = localSnapshot.getProcessId();

            int numberOfServers = MultiplePredicateMonitor.getNumberOfServers();

            // declaration of variables may of use
            Vector<InternalBuffer> internalBufferArray = null;
            Vector<LinkedBlockingQueue<LocalSnapshot>> candidateQueueArray = null;
            GlobalSnapshot gs = null;
            Predicate pred = null;

            synchronized (sharedLockForReceiverAndProcessor) {
                int predStatus = getPredicateStatusAtMonitor(
                        predicateName,
                        monitorPredicateStatus,
                        MultiplePredicateMonitor.getPredicateExpirationTime(),
                        predicateInternalBufferMap,
                        predicateCandidateQueueArrayMap);

                switch (predStatus) {
                    case MONITOR_PREDICATE_STATUS_NA:
                        // new predicate
                        // create new data structure for this predicate

                        // internal buffer
                        internalBufferArray = new Vector<>(numberOfServers);
                        for (int i = 0; i < numberOfServers; i++) {
                            internalBufferArray.addElement(new InternalBuffer());
                        }
                        predicateInternalBufferMap.put(predicateName, internalBufferArray);

                        // candidateQueueArray
                        candidateQueueArray = new Vector<>(numberOfServers);
                        for (int i = 0; i < numberOfServers; i++) {
                            candidateQueueArray.addElement(new LinkedBlockingQueue<>());
                        }
                        predicateCandidateQueueArrayMap.put(predicateName, candidateQueueArray);

                        // globalSnapshot
                        gs = new GlobalSnapshot(numberOfServers);
                        predicateGlobalSnapshotMap.put(predicateName, gs);

                        // predicateList
                        // we infer the predicate from predicateName
                        // we do not care about monitor address in this case
                        // pred = Predicate.generatePredicateFromName(predicateName, null, null);
                        pred = Predicate.generatePredicateFromNameEnhanced(predicateName, null, null);
                        monitorPredicateList.put(predicateName, pred);

                        // monitorPredicateStatus
                        // new predicate is active
                        markPredicateStatusActive(predicateName, monitorPredicateStatus);

                        break;

                    case MONITOR_PREDICATE_STATUS_ACTIVE:
                        // existing and active predicate

                        // retrieve appropriate internal buffer and queue array
                        internalBufferArray = predicateInternalBufferMap.get(predicateName);
                        candidateQueueArray = predicateCandidateQueueArrayMap.get(predicateName);

                        // Duong debug
                        if (internalBufferArray == null) {
                            System.out.println(" ERROR ACTIVE internalBufferArray for " + predicateName + " is null");
                        }
                        if (candidateQueueArray == null) {
                            System.out.println(" ERROR ACTIVE candidateQueueArray for " + predicateName + " is null");
                        }

//                    internalBufferArray.elementAt(processId).insertAndDeliverLocalSnapshot(
//                            predicateName,
//                            localSnapshot,
//                            candidateQueueArray.elementAt(processId));

                        break;

                    case MONITOR_PREDICATE_STATUS_INACTIVE:

                        // existing but inactive predicate

                        // As new candidate arrive, inactive predicate will become active.
                        // What if new candidate indicate the predicate is inactive (e.g. it contains
                        // infinite hvc endpoint)? then the predicate will be marked as inactive later
                        // by CandidateProcessing thread
                        // Mark as active
                        markPredicateStatusActive(predicateName, monitorPredicateStatus);

                        // retrieve appropriate internal buffer and queue array
                        internalBufferArray = predicateInternalBufferMap.get(predicateName);
                        candidateQueueArray = predicateCandidateQueueArrayMap.get(predicateName);

                        // Duong debug
                        if (internalBufferArray == null) {
                            System.out.println(" ERROR INACTIVE internalBufferArray for " + predicateName + " is null");
                        }
                        if (candidateQueueArray == null) {
                            System.out.println(" ERROR INACTIVE candidateQueueArray for " + predicateName + " is null");
                        }

//                    internalBufferArray.elementAt(processId).insertAndDeliverLocalSnapshot(
//                            predicateName,
//                            localSnapshot,
//                            candidateQueueArray.elementAt(processId));

                        break;

                    case MONITOR_PREDICATE_STATUS_EXPIRED:
                        // expired predicate should have its data structures removed before,
                        // so we have to recreate those

                        // internal buffer
                        internalBufferArray = new Vector<>(numberOfServers);
                        for (int i = 0; i < numberOfServers; i++) {
                            internalBufferArray.addElement(new InternalBuffer());
                        }
                        predicateInternalBufferMap.put(predicateName, internalBufferArray);

                        // candidateQueueArray
                        candidateQueueArray = new Vector<>(numberOfServers);
                        for (int i = 0; i < numberOfServers; i++) {
                            candidateQueueArray.addElement(new LinkedBlockingQueue<>());
                        }
                        predicateCandidateQueueArrayMap.put(predicateName, candidateQueueArray);

                        // predicateList
                        // we infer the predicate from predicateName
                        //pred = Predicate.generatePredicateFromName(predicateName, null, null);
                        pred = Predicate.generatePredicateFromNameEnhanced(predicateName, null, null);
                        monitorPredicateList.put(predicateName, pred);

                        // globalSnapshot should not be the dump one but the inactive one
                        gs = new GlobalSnapshot(numberOfServers);
                        // change from dump snapshot content to inactive content
                        for (int serverId = 0; serverId < numberOfServers; serverId++) {
                            LocalSnapshotContent lsc = new LocalSnapshotContent(pred.generateInactiveMapping());
                            HVC zeroHvc = new HVC(numberOfServers, getEpsilon(), serverId, 0L);
                            HVC infinityHvc = new HVC(numberOfServers, getEpsilon(), serverId, Integer.MAX_VALUE);
                            HvcInterval hvcInterval = new HvcInterval(0L, zeroHvc, infinityHvc);

                            // create local snapshot
                            // Note seqNumber = 0 since its value is not relevant
                            LocalSnapshot ls = new LocalSnapshot(hvcInterval, 0L, lsc);

                            gs.replaceCandidate(ls, serverId);
                        }

                        predicateGlobalSnapshotMap.put(predicateName, gs);

                        // mark status as active again
                        markPredicateStatusActive(predicateName, monitorPredicateStatus);


//                    // put local snapshot into internal buffer before deliver to queue
//                    internalBufferArray.elementAt(processId).insertAndDeliverLocalSnapshot(
//                            predicateName,
//                            localSnapshot,
//                            candidateQueueArray.elementAt(processId));

                        break;
                }
            }

            // put local snapshot into internal buffer before deliver to queue
            internalBufferArray.elementAt(processId).insertAndDeliverLocalSnapshot(
                    predicateName,
                    localSnapshot,
                    candidateQueueArray.elementAt(processId));



        }

    }

}
