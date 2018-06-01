package predicatedetectionlib.common;

import predicatedetectionlib.monitor.InternalBuffer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.AbstractMap;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Common values used in the project, for both server, client, and monitor
 * Created by duongnn on 6/12/17.
 */
public class CommonUtils {
    // port number where the monitor listen for incoming messages from servers
    public static final int SERVER_TO_MONITOR_PORT_NUMBER = 3308;
    // port number for communication between monitors
    public static final int MONITOR_TO_MONITOR_PORT_NUMBER = 8033;

    public static final int MONITOR_PREDICATE_STATUS_NA = -1;       // predicate not available
    public static final int MONITOR_PREDICATE_STATUS_ACTIVE = 0;    // still active
    public static final int MONITOR_PREDICATE_STATUS_INACTIVE = 1; // is inactive for a short time
    public static final int MONITOR_PREDICATE_STATUS_EXPIRED = 2;  // has been inactive for a long time

    public static void log(String str) {
        System.out.println(str);
    }

    public static void catFile(String filename){
        System.out.println("content of file: " + filename);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));

            String line = reader.readLine();

            while(line != null){
                System.out.println("  " + line);
                line = reader.readLine();
            }

        }catch(Exception e){
            System.out.println(" CommonUtils.catFile ERROR " + e.getMessage());
            e.printStackTrace();
        }
    }


    // return non loopback local adddress
    //   we need non loopback address so that other machines could connect to the monitor
    //   loopback address 127.0.0.1 or "localhost" is reachable only by processes on the same machine
    public static InetAddress getLocalNonLoopbackAddress() throws SocketException {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while( ifaces.hasMoreElements() ) {
            NetworkInterface iface = ifaces.nextElement();
            Enumeration<InetAddress> addresses = iface.getInetAddresses();

            while( addresses.hasMoreElements() ) {
                InetAddress addr = addresses.nextElement();
                if( addr instanceof Inet4Address && !addr.isLoopbackAddress() ) {
                    return addr;
                }
            }
        }

        return null;
    }

    /**
     * Get a string representing a socket channel of the form localAddress::remoteAddress
     * Example: /35.9.26.198:8000::/35.9.26.198:38643
     * @param socketChannel the socket channel of interest
     * @return the desired string or null if error
     */
    public static String getStringRepresentSocketChannel(SocketChannel socketChannel){
        String socketChannelStr = null;
        try{
            socketChannelStr = socketChannel.getLocalAddress().toString() +
                    "::" +
                    socketChannel.getRemoteAddress().toString();
        }catch(IOException ioe){
            System.out.println("getStringRepresentSocketChannel ERROR: when convert channel to string");
        }
        return socketChannelStr;
    }

    /**
     * determine the status of a predicate
     * @param predName name of predicate of interest
     * @param monitorPredicateStatus
     *          a hashmap<String, Long> that maps predicate name to the time of inactive
     *          of time of inactive = 0 => still active
     *                              > 0 => inactive
     *                              > 0 and > expirationThreshold => expired
     * @param expirationThreshold
     * @return NA if not exists in predicate status list
     *         If exists in predicate status list
     *              Active if value is 0
     *              Inactive if value != 0
     */
    public static int getPredicateStatusAtMonitor(String predName,
                                                  AbstractMap<String, Long> monitorPredicateStatus,
                                                  long expirationThreshold,
                                                  ConcurrentHashMap<String, Vector<InternalBuffer>> predicateInternalBufferMap,
                                                  ConcurrentHashMap<String, Vector<LinkedBlockingQueue<LocalSnapshot>>> predicateCandidateQueueArrayMap){

        Long inactiveTime;
        if((inactiveTime = monitorPredicateStatus.get(predName)) == null) {
            // not exists
            return MONITOR_PREDICATE_STATUS_NA;

        }else{
            // predicate already exists
            if (inactiveTime == 0) {
                // still active
                return MONITOR_PREDICATE_STATUS_ACTIVE;
            } else {
                // inactive
                if (System.currentTimeMillis() - inactiveTime < expirationThreshold) {
                    // inactive but not exprired
                    return MONITOR_PREDICATE_STATUS_INACTIVE;
                } else {
                    // inactive more than threshold

                    // to be considered as expired, internal bufffer need to be empty
                    Vector<InternalBuffer> predicateInternalBuffer = predicateInternalBufferMap.get(predName);
                    Vector<LinkedBlockingQueue<LocalSnapshot>> predicateCandidateQueueArray = predicateCandidateQueueArrayMap.get(predName);

                    if(predicateInternalBuffer == null || predicateCandidateQueueArray == null)
                        return MONITOR_PREDICATE_STATUS_EXPIRED;

                    for(int i = 0; i < predicateInternalBuffer.size(); i++){
                        if(!predicateInternalBuffer.elementAt(i).getUnderlyingVector().isEmpty()){
                            return MONITOR_PREDICATE_STATUS_INACTIVE;
                        }
                    }

                    // and candidate queue array must be empty too
                    for(int i = 0; i < predicateCandidateQueueArray.size(); i++){
                        if(! predicateCandidateQueueArray.elementAt(i).isEmpty()){
                            return MONITOR_PREDICATE_STATUS_INACTIVE;
                        }
                    }

                    return MONITOR_PREDICATE_STATUS_EXPIRED;
                }
            }
        }
    }
}
