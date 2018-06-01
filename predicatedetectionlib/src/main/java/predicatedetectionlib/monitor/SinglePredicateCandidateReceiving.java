//package predicatedetectionlib.monitor;
//
//import predicatedetectionlib.common.ByteUtils;
//import predicatedetectionlib.common.CommonUtils;
//import predicatedetectionlib.common.LocalSnapshot;
//import predicatedetectionlib.common.LocalSnapshotContent;
//
//import java.net.*;
//import java.nio.ByteBuffer;
//import java.nio.channels.SelectionKey;
//import java.nio.channels.Selector;
//import java.nio.channels.ServerSocketChannel;
//import java.nio.channels.SocketChannel;
//import java.util.Iterator;
//import java.util.Set;
//import java.util.Vector;
//import java.util.concurrent.LinkedBlockingQueue;
//
///**
// * Created by duongnn on 6/21/17.
// */
//
//public class SinglePredicateCandidateReceiving implements Runnable{
//
//    private final long missingCandidateTimeOut; // if missing candidate has not been received by that time
//                                                // we can believe it is missing
//
//    // This LinkedBlockingQueue is shared with consumer process
//    private final Vector<LinkedBlockingQueue<LocalSnapshot>> candidateQueueArray;
//
//    // a private buffer used only by the producer
//    //private Vector<Vector<LocalSnapshot>> internalBufferVector;
//    private Vector<InternalBuffer> internalBufferVector;
//
//    SinglePredicateCandidateReceiving(Vector<LinkedBlockingQueue<LocalSnapshot>> aQueueArray){
//        candidateQueueArray = aQueueArray;
//
//        // init internal buffer
//        internalBufferVector = new Vector<>(SinglePredicateMonitor.getNumberOfServers());
//        for (int i = 0; i < SinglePredicateMonitor.getNumberOfServers(); i++){
//            internalBufferVector.addElement(new InternalBuffer());
//        }
//
//        missingCandidateTimeOut = 2000; // 2000 milliseconds
//    }
//
//    public void run() {
//        try {
//            // Selector: multiplexor of SelectableChannel objects
//            Selector selector = Selector.open(); // selector is open here
//
//            // ServerSocketChannel: selectable channel for stream-oriented listening sockets
//            ServerSocketChannel monitorSocket = ServerSocketChannel.open();
//            InetSocketAddress monitorAddr = new InetSocketAddress(CommonUtils.getLocalNonLoopbackAddress(), SinglePredicateMonitor.getMonitorPortNumber());
//
//            // Binds the channel's socket to a local address and configures the socket to listen for connections
//            monitorSocket.bind(monitorAddr);
//
//            // Adjusts this channel's blocking mode to non-blocking
//            monitorSocket.configureBlocking(false);
//
//            // set timeout so that it won't close too early
//            // this is important so that the connection between server and monitor is persistent
//            monitorSocket.socket().setSoTimeout(0);
//
//            int ops = monitorSocket.validOps();
//            SelectionKey selectKey = monitorSocket.register(selector, ops, null);
//
//            CommonUtils.log("\n");
//            CommonUtils.log("Candidate receiving process:");
//            CommonUtils.log("          numberOfServer = " + SinglePredicateMonitor.getNumberOfServers());
//            CommonUtils.log("                 epsilon = " + SinglePredicateMonitor.getEpsilon());
//            CommonUtils.log(" waiting for msg address = " + monitorAddr.getAddress() + ":" + SinglePredicateMonitor.getMonitorPortNumber()); //SinglePredicateMonitor.getMonitorPortNumber());
//            CommonUtils.log("\n");
//
//            // Infinite loop..
//            // Keep server running
//
////            while (true) {
//            while (!SinglePredicateMonitor.stopThreadNow) {
//
//                // Selects a set of keys whose corresponding channels are ready for I/O operations
//                selector.select();
//
//                // token representing the registration of a SelectableChannel with a Selector
//                Set<SelectionKey> selectedKeys = selector.selectedKeys();
//                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
//
//                while (keyIterator.hasNext()) {
//                    SelectionKey currentKey = keyIterator.next();
//
//                    // Tests whether this key's channel is ready to accept a new socket connection
//                    if (currentKey.isAcceptable()) {
//                        SocketChannel voldServer = monitorSocket.accept();
//
//                        // Duong debug important
//                        voldServer.socket().setSoTimeout(0);
//
//                        // Adjusts this channel's blocking mode to false
//                        voldServer.configureBlocking(false);
//
//                        // Operation-set bit for read operations
//                        voldServer.register(selector, SelectionKey.OP_READ);
//
//                        //CommonUtils.log("Connection Accepted: " + voldServer.getLocalAddress() + "\n");
//
//                        // Tests whether this key's channel is ready for reading
//                    } else if (currentKey.isReadable()) {
//
//                        SocketChannel voldServer = (SocketChannel) currentKey.channel();
//                        final int RESERVED_SPACE = 100;
//                        int MAX_BUFFER_SIZE = ByteUtils.SIZE_OF_CHAR * 20 // size of predicateName
//                                + ByteUtils.SIZE_OF_INT * 3
//                                + ByteUtils.SIZE_OF_LONG
//                                + ByteUtils.SIZE_OF_LONG * LocalSnapshot.MAX_NUMBER_OF_PROCESSES
//                                + ByteUtils.SIZE_OF_INT * LocalSnapshotContent.MAX_NUMBER_OF_ROWS
//                                + LocalSnapshotContent.MAX_NUMBER_OF_ROWS * LocalSnapshotContent.MAX_COLUMN_STRING_LENGTH * ByteUtils.SIZE_OF_CHAR
//                                + RESERVED_SPACE;
//
//
//                        ByteBuffer messageBuffer = ByteBuffer.allocate(MAX_BUFFER_SIZE);
//                        voldServer.read(messageBuffer);
//
//                        // Duong debug important
//                        // not sure if flip is appropriate here
//                        messageBuffer.flip();
//
//                        // construct LocalSnapshot from byte buffer
//                        LocalSnapshot localSnapshot = new LocalSnapshot();
//
//                        // Duong debug
//                        localSnapshot.setReceivedTime(System.currentTimeMillis());
//
////                        CommonUtils.log("  producer received message with " + messageBuffer.remaining() +
////                                " bytes (max = " + MAX_BUFFER_SIZE + " bytes)" +
////                                " from node " + voldServer.getRemoteAddress());
//
//                        int numOfBytes = localSnapshot.fromBytes(messageBuffer.array(), 0);
//
////                        CommonUtils.log("    " + numOfBytes + " bytes converted to localSnapshot of node " + localSnapshot.getProcessId());
////                        CommonUtils.log(localSnapshot.toString(0));
//
//                        // put snapshot into buffer before deliver to queue
//                        int bufferId = localSnapshot.getProcessId();
//
//                        insertLocalSnapshotIntoInternalBuffer(localSnapshot, bufferId);
//
//
//                        // imporant: disable closing connection so that connection is persistent
////                        // close connection
////                        voldServer.close();
//                    }
//                    keyIterator.remove();
//                }
//            }
//
//            // Duong debug
//            System.out.println("SinglePredicateCandidateReceiving thread ended");
//
//        }catch(Exception e){
//            CommonUtils.log(e.getMessage());
//            e.printStackTrace();
//            throw new RuntimeException();
//        }
//
//
//    }
//
//    /*  Insert a local snapshot into specified internal buffer
//        Since snapshot could arrive in order not consistent with their sequence number,
//        i.e. higher seq. number one may arrive earlier. Thus we should not put
//        snapshot immediately into linked blocking queue.
//        We should put snapshot into a local buffer first where snapshots are ordered by their
//        sequence numbers.
//        After each insert, we check if the expected seq. number has received
//          if yes, flush them into the queue and update expected seq. number
//          if no, then do not flush the snapshot into the queue yet
//
//        note: we also need to ignore sequence numbers that pass the expected
//        since we arrived after the timeout, and we assume they are lost
//
//     */
//    private synchronized void insertLocalSnapshotIntoInternalBuffer(LocalSnapshot localSnapshot, int bufferId){
//        InternalBuffer specifiedBuffer = internalBufferVector.elementAt(bufferId);
//        Vector<LocalSnapshot> specifiedBufferVector = specifiedBuffer.getUnderlyingVector();
//
//        // if the packet has expired, ignore
//        if(specifiedBuffer.getExpectedSequenceNumber() > localSnapshot.getSequenceNumber()){
//            return;
//        }
//
//        if(specifiedBufferVector.isEmpty()){
//            specifiedBufferVector.addElement(localSnapshot);
//        }else{
//            // find your position to insert
//            long newSequenceNumber = localSnapshot.getSequenceNumber();
//            int currentPosition = 0;
//            long currentSequenceNumber = specifiedBufferVector.elementAt(currentPosition).getSequenceNumber();
//            while(currentSequenceNumber < newSequenceNumber){
//                currentPosition ++;
//                if(currentPosition >= specifiedBufferVector.size()) {
//                    // reach end of vector
//                    break;
//                }
//
//                currentSequenceNumber = specifiedBufferVector.elementAt(currentPosition).getSequenceNumber();
//            }
//
//            // insert element at currentPosition
//            // it should shift elements with higher seq. number
//            specifiedBufferVector.insertElementAt(localSnapshot, currentPosition);
//        }
//
//        flushInternalBuffer(specifiedBuffer, bufferId);
//
//    }
//
//    // if the heading of the internal buffer is ready, deliver them, i.e. put them into the queue
//    private synchronized void flushInternalBuffer(InternalBuffer specifiedBuffer, int queueId){
//        // check if the first element sequence number match with the one expected
//        Vector<LocalSnapshot> specifiedBufferVector = specifiedBuffer.getUnderlyingVector();
//
//        if(specifiedBufferVector.isEmpty())
//            return;
//
//        LocalSnapshot firstLocalSnapshot = specifiedBufferVector.firstElement();
//        long firstSequenceNumber = firstLocalSnapshot.getSequenceNumber();
//
//        // Duong debug
////        CommonUtils.log("  + Buffer " + queueId + ": " +
////                " first = " + firstSequenceNumber +
////                " expect = " + specifiedBuffer.getExpectedSequenceNumber());
//
//        // in case some sequence numbers are missing, we need to check whether it is on the way
//        // or it has been lost
//        if(firstSequenceNumber > specifiedBuffer.getExpectedSequenceNumber()){
//            // how long the first one has been waiting?
//            long firstLocalSnapshotWaitTime = System.currentTimeMillis() - firstLocalSnapshot.getReceivedTime();
//
//            // if that waiting time exceed timeout, the sequence numbers before it are likely
//            // be lost, we should not wait for those any more
//            if(firstLocalSnapshotWaitTime > missingCandidateTimeOut){
//                specifiedBuffer.jumpExpectedSequenceNumber(firstSequenceNumber);
//            }
//        }
//
//        while(firstSequenceNumber == specifiedBuffer.getExpectedSequenceNumber()){
//            // put the first element into the queue
//            if(candidateQueueArray.elementAt(queueId).offer(firstLocalSnapshot) == true){
//                CommonUtils.log("     ++ Producer: localSnapshot seq " + firstSequenceNumber +
//                        " inserted into candidateQueueArray[" + queueId + "]");
//            }else{
//                CommonUtils.log("     ++ Producer: The candidateQueueArray[" + queueId + "] is full");
//
//                // just quit and try in another time
//                break;
//            }
//
//            // remove first from the vector
//            specifiedBufferVector.removeElementAt(0);
//
//            // increment expected sequence number
//            specifiedBuffer.incrementExpectedSequenceNumber();
//
//            // if buffer is empty, no more flushing
//            if(specifiedBufferVector.isEmpty())
//                break;
//
//            // move to the next element if possible (i.e. buffer is not empty yet)
//            firstLocalSnapshot = specifiedBufferVector.firstElement();
//            firstSequenceNumber = firstLocalSnapshot.getSequenceNumber();
//        }
//    }
//
//}
