package predicatedetectionlib.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;

/**
 * Hybrid Vector Clock
 *
 * Note that the (hybrid) vector clock implemented by this class is different from
 * the vector-clock originally provided by Voldemort.
 *
 * The vector-clock in Voldemort is vector-clock version, meaning that it is used
 * for recording the versioning of data items. Its purpose is to help resolving
 * conflicting versions of same data items.
 *
 * The vector clock implemented by this class is for recording the time of
 * events. Its purpose is to help determining causal relationship between events,
 * and thus could be helpful in predicate detection.
 *
 * Events consists of send, receive, and internal events. We will try to capture all
 * send and receive events at server by adding HVC code to AsynchRequestHandler.
 *
 * Since I do not have complete list of internal events (and that list should be very
 * long), I just add HVC code to interested internal events.
 *
 * Created by duongnn on 5/23/17.
 */
public class HVC implements Serializable {
    // constants for comparison between 2 HVCs
    // they are the same as constants in Voldemort.versioning.Occurred
    // but this one has INCOMPATIBLE constant, thus will re-define them here
    public static final int BEFORE = 1;
    public static final int AFTER = -1;
    public static final int CONCURRENT = 0;
    public static final int INCOMPATIBLE = -2;  // when 2 HVCs of different size

    public static final long INFINITY_TIMESTAMP = Long.MAX_VALUE / HvcElement.RESOLUTION_FACTOR; // 9,223,372,036,854 . 775807

    // meta-data information
    // private int size; // number of processes involving in HVC. Since this value depends
                         // on hvcTimestamp.size(), we remove it to avoid redundancy
    private final int nodeId;
    private int activeSize; // number of non-default entries in HVC
    private final long epsilon; // uncertainty window, in milliseconds

    // only hvcTimestamp are communicated between servers
//    private Vector<Long> hvcTimestamp;   // we choose Vector instead of ArrayList because
//                                            // vector in Java is synchronized

    // since millisecond is not enough of resolution, we have to use special
    // data type for timestamp
    private Vector<HvcElement> hvcTimestamp;

    // this constructor is used to create initial HVC at servers
    public HVC(int size, long epsilon, int nodeId, long physicalTimeMs){
        this.nodeId = nodeId;
        this.epsilon = epsilon;

        this.hvcTimestamp = new Vector<HvcElement>(size);
        for(int i = 0; i < size; i++){
            // since Vector hvcTimestamp has not yet been initialized, if we set value
            // of the vector with setElementAt()
            //      hvcTimestamp.setElementAt(physicalTime - epsilon, i);
            // we will get the error:
            //      java Exception in thread "main" java.lang.ArrayIndexOutOfBoundsException: 0 >= 0
            // we should use addElement
//            hvcTimestamp.addElement(new Long(physicalTime - epsilon));

            HvcElement hvcElement = new HvcElement();
            hvcElement.setTimeValueInMs(physicalTimeMs - epsilon);

            hvcTimestamp.addElement(hvcElement);
        }

        // just as a continuation of the above discussion on ArrayIndexOutOfBoundsException:
        // after elements are added, you can modify them with setElementAt() or set()
//        for(int i = 0; i < size; i++){
//            hvcTimestamp.setElementAt(physicalTime - epsilon, i);
//        }

//        this.hvcTimestamp.set(nodeId, physicalTime);
        this.hvcTimestamp.elementAt(nodeId).setTimeValueInMs(physicalTimeMs);

        activeSize = 1;

    }

    // this constructor is used to create HVC from message server sent to monitor
    public HVC(int nodeId, long epsilon, Vector<HvcElement> hvcTimestamp){
        this.nodeId = nodeId;
        this.epsilon = epsilon;
        this.hvcTimestamp = hvcTimestamp;
        updateActiveSize();
    }

    // constructor that copies content from another HVC
    public HVC(HVC someHvc){
        this.nodeId = someHvc.getNodeId();
        this.epsilon = someHvc.getEpsilon();
        this.activeSize = someHvc.getActiveSize();

        int size = someHvc.getHvcTimestamp().size();
        this.hvcTimestamp = new Vector<HvcElement>(size);

        for(int i = 0; i < size; i++){
            //hvcTimestamp.addElement(new Long(someHvc.getHvcTimestamp().elementAt(i)));
            HvcElement hvcElement = new HvcElement();
            hvcElement.setTimeValue(someHvc.getHvcTimestamp().elementAt(i).getTimeValue());
            hvcTimestamp.addElement(hvcElement);
        }
    }

    /**
     * A constructor to create HVC from the timestamp on the message
     * This constructor is also used by client to create initial HVC
     * or to update client's HVC from message
     * @param timestamp
     */
    public HVC(Vector<HvcElement> timestamp){
        // copy the timestamp
        this.hvcTimestamp = timestamp;
        //this.size = timestamp.size();

        // Other variables are not relevant
        // We assign some values to final variables to make the code compile
        this.nodeId = -1;
        this.epsilon = 1000;
    }

    public static HVC getInfinityHVC(int size, long epsilon, int nodeId){
        return new HVC(size, epsilon, nodeId, INFINITY_TIMESTAMP);
    }

    public boolean isInfinityHVC(){
        return hvcTimestamp.elementAt(nodeId).getTimeValueInMs() == INFINITY_TIMESTAMP;
    }

    /*
      We should make sure all methods reading/writing to HVC elements are
      synchronized
      The reason is that the server have multiple threads to serve asynchronous requests
      and they may access HVC concurrently.
     */

    public synchronized int getSize(){
        //return size;
        return hvcTimestamp.size();
    }

    // since nodeId is final, no need for synchronization
    public int getNodeId(){
        return nodeId;
    }

    public HvcElement getPrimaryElement(){
        return hvcTimestamp.elementAt(nodeId);
    }

    public synchronized int getActiveSize(){
        return activeSize;
    }

    public synchronized Vector<HvcElement> getHvcTimestamp(){
        return hvcTimestamp;
    }

    public synchronized long getEpsilon(){
        return epsilon;
    }

    public synchronized void updateActiveSize(){
        int count = 0;
//        long primaryElement = hvcTimestamp.elementAt(nodeId);
        HvcElement primaryElement = hvcTimestamp.elementAt(nodeId);

        // since epsilon unit is millisecond, we use millisecond values
        for(int i = 0; i < hvcTimestamp.size(); i++){
            if(hvcTimestamp.elementAt(i).getTimeValueInMs() > primaryElement.getTimeValueInMs() - epsilon){
                // updated value
                count ++;
            }else{
                // outdated value => need to be changed to default value
                // hvcTimestamp.setElementAt(primaryElement - epsilon, i);
                hvcTimestamp.elementAt(i).setTimeValueInMs(primaryElement.getTimeValueInMs() - epsilon);
            }
        }
        activeSize = count;
    }

    public synchronized void setHvcTimestamp(Vector<HvcElement> hvcTimestamp){
        this.hvcTimestamp = hvcTimestamp;
        // this.size = hvcTimestamp.size();

        // since setHvcTimestamp is called by client too, we should not allow
        // the call updateActive() this method will access nodeId which is -1
        // for client, and thus cause ArrayIndexOutOfBoundException -1
        // updateActiveSize();

        // should not change your nodeId and epsilon
    }

    public synchronized void setHvcElementMs(long valueMs, int position){
//        hvcTimestamp.set(position, valueMs);
        hvcTimestamp.elementAt(position).setTimeValueInMs(valueMs);
    }

    public synchronized void setHvcElement(long value, int position){
        hvcTimestamp.elementAt(position).setTimeValue(value);
    }

    public synchronized void setHvcElement(HvcElement hvcElement, int position){
        hvcTimestamp.set(position, hvcElement);
    }

    /**
     * compare this HVC with another HVC
     * @param they another HVC
     * @param twoEpsilon the uncertainty windows
     *                Although each HVC has an epsilon associated with it (to help calculating active size)
     *                we use an independent epsilon provided by the monitor in checking causal relationship
     *                This allow us more flexibility
     * @return
     *  1   if this HVC happens before other HVC
        -1  if other HVC happens before this HVC
        0   if concurrent (neither one happens before the other)
        -2  if error
     */

    public synchronized int happenBefore(HVC they, long twoEpsilon){
        Boolean youHappenBefore = true;
        Boolean theyHappenBefore = true;
        int i;

        // make sure they have same size
        if(they.getSize() != getSize()){
            System.out.println(" Two HVCs are not compatible");
            return INCOMPATIBLE;
        }

        for (i = 0; i < getSize(); i++){
            if(hvcTimestamp.elementAt(i).getTimeValue() < they.getHvcTimestamp().elementAt(i).getTimeValue())
                theyHappenBefore = false;
            if(hvcTimestamp.elementAt(i).getTimeValue() > they.getHvcTimestamp().elementAt(i).getTimeValue())
                youHappenBefore = false;
        }

        /* result
                                       theyHappenBefore
                                    T              F
                                  +-----------+-----------+
                                T |CONCURRENT |BEFORE     |
             youHappenBefore      +-----------+-----------+
                                F |AFTER      |CONCURRENT |
                                  +-----------+-----------+
        */

        if(youHappenBefore == theyHappenBefore) {
            // in case of VC, this is enough to conclude concurrent
            // but in case of HVC, this is not enough yet
            // return CONCURRENT;

            // we have to check the primary element in each HVC so see if they are
            // within 2*epsilon uncertainty windows, they are concurrent.
            // since epsilon unit is millisecond, we will use millisecond value
            long myPrimaryClockMs = this.getPrimaryElement().getTimeValueInMs();
            long theyPrimaryClockMs = they.getPrimaryElement().getTimeValueInMs();

            if(myPrimaryClockMs < theyPrimaryClockMs - twoEpsilon){
                return BEFORE;
            }

            if(theyPrimaryClockMs < myPrimaryClockMs - twoEpsilon){
                return AFTER;
            }

            return CONCURRENT;

        }

        if(youHappenBefore)
            // you really happen before other HVC
            return BEFORE;
        else
            // they happens before you
            return AFTER;

    }

    /**
     *
     */
    public void writeHvcToStream(DataOutputStream outputStream){
        int numOfEntries = getSize();

        try {
            // write the number of entries
            outputStream.writeShort(numOfEntries);

            // write the timestamp
            for (int i = 0; i < numOfEntries; i++) {
                outputStream.writeLong(getHvcTimestamp().elementAt(i).getTimeValue());
            }

        }catch(IOException e){
            throw new IllegalArgumentException("Can't serialize vectorclock to stream", e);
        }

    }


    /**
     * read vector clock timestamp from data input stream
     *  format:
     *      +---------------+-------------------------------+
     *      |# of entries   | #_of_entries * SIZE_OF_LONG   |
     *      |(short)        | each entry is a long          |
     *      +---------------+-------------------------------+
     * @param inputStream
     * @return  if number of entries = 0, return null
     *          otherwise, an HVC instance corresponding the the byte stream
     */
    public static HVC readHvcFromStream(DataInputStream inputStream){
        try {
            final int HEADER_LENGTH = ByteUtils.SIZE_OF_SHORT;
            byte[] header = new byte[HEADER_LENGTH];
            inputStream.readFully(header);

            int numEntries = ByteUtils.readShort(header, 0);

            // Duong debug
            //System.out.println(" readHvcFromstream: has " + numEntries + " entries");

            int entrySize = ByteUtils.SIZE_OF_LONG;
            int totalEntrySize = numEntries * entrySize;

            if(numEntries == 0) {
                // a dump HVC used by client
                return null;
            }

            // Duong debug
            //System.out.println("HVC.readHvcFromStream(): numEntries = " + numEntries + " entrySize = " + entrySize);

            byte[] HvcBytes = new byte[HEADER_LENGTH + totalEntrySize];

            // we cannot re-read the input stream
            // thus we have to copy the bytes we have read
            System.arraycopy(header, 0, HvcBytes, 0, header.length);

            inputStream.readFully(HvcBytes, HEADER_LENGTH, HvcBytes.length
                    - HEADER_LENGTH);

            return new HVC(readHvcTimestampFromBytes(HvcBytes));
        } catch(IOException e) {
            throw new IllegalArgumentException("Can't deserialize vectorclock from stream", e);
        }

    }

    public static Vector<HvcElement> readHvcTimestampFromBytes(byte[] bytes){
        return readHvcTimestampFromBytes(bytes, 0);
    }

    /**
     *
     * read vector clock from a byte array
     *  format:
     *      +---------------+-------------------------------+
     *      |# of entries   | #_of_entries * SIZE_OF_LONG   |
     *      |(short)        | each entry is a long          |
     *      +---------------+-------------------------------+
     *                        In Voldemort, an entry could be
     *                        formated in less than 8 bytes since
     *                        their values are small (logical clock)
     *                        In HVC, values come from physical clock
     *                        thus we always need 8 bytes
     *
     * @param bytes : the byte array
     * @return an HVC instance
     */
    public static Vector<HvcElement> readHvcTimestampFromBytes(byte[] bytes, int offset){
        if(bytes == null || bytes.length <= offset)
            throw new IllegalArgumentException("Invalid byte array for serialization--no bytes to read.");

        int numEntries = ByteUtils.readShort(bytes, offset);
        int entrySize = ByteUtils.SIZE_OF_LONG;
        int minimumBytes = offset + ByteUtils.SIZE_OF_SHORT + numEntries * entrySize;
        if(bytes.length < minimumBytes)
            throw new IllegalArgumentException("Too few bytes: expected at least " + minimumBytes
                    + " but found only " + bytes.length + ".");

        Vector<HvcElement> timestamp = new Vector<HvcElement>(numEntries);
        int index = offset + ByteUtils.SIZE_OF_SHORT;

        // Duong debug
        //System.out.println(" HVC.readHvcTimestampFromBytes(): numEntries = " + numEntries + " entrySize = "
        //                    + entrySize);

        for(int i = 0; i < numEntries; i++){
            long elementTime = ByteUtils.readLong(bytes, index);
//            timestamp.addElement(elementTime);
            HvcElement hvcElement = new HvcElement();
            hvcElement.setTimeValue(elementTime);
            timestamp.addElement(hvcElement);

            // do not use
            // timestamp.set(i, elementTime);
            // since the vector is not yet initialized, thus using set() will cause
            // ArrayIndexOutOfBoundsException

            index += entrySize;
        }

        return timestamp;
    }

//    /**
//     * return the serialization of vector clock in an array of bytes
//     * @return
//     */
//    public synchronized byte[] toBytes(){
//        // TODO
//        return null;
//    }

    /**
     * write the serialization of vector clock into a buffer
     * return the number of bytes written
     * @param buf
     * @param offset
     * @return
     */
//    public synchronized int toBytes(byte[] buf, int offset) {
//        // TODO
//        return 0;
//    }
//
//    public synchronized int sizeInBytes(){
//        // TODO
//        return 0;
//    }

    /**
     * Set my local clock (i.e. the j-th element in HVC of process j) to the wallclock.
     * HVC local clock in milliseconds should never exceed wallclock in milliseconds
     * Other element of HVC are also updated if needed, i.e. elements less than hvc(j) - epsilon
     * will be set to hvc(j) - epsilon
     */
    public synchronized void incrementMyLocalClock(){
        // your HVC element will be the max of physical clock or your current time + 1
        long physicalTimeMs = System.currentTimeMillis();
        long currentLocalClockMs = hvcTimestamp.elementAt(nodeId).getTimeValueInMs();

//        // Duong debug
//        System.out.println("Increment local clock");
//        Thread.dumpStack();
//        System.out.println(" -- Before increment: ");
//        System.out.println(toStringHvcFull(2));

//        long newLocalClock = Math.max(currentLocalClock + 1, physicalTime);
//
//
//        hvcTimestamp.setElementAt(newLocalClock, nodeId);
//
//        // update elements that you have not heard of for a long time
//        for(int i = 0; i < getSize(); i++){
//            if(hvcTimestamp.elementAt(i) < newLocalClock - epsilon) {
//                hvcTimestamp.setElementAt(newLocalClock - epsilon, i);
//            }
//        }

        long newLocalClockMs;
        if(currentLocalClockMs < physicalTimeMs){
            newLocalClockMs = physicalTimeMs;
            hvcTimestamp.elementAt(nodeId).setTimeValueInMs(physicalTimeMs);

        }else{
            long newLocalClockNs = hvcTimestamp.elementAt(nodeId).getTimeValue() + 1;
            hvcTimestamp.elementAt(nodeId).setTimeValue(newLocalClockNs);

            // newLocalClockMs is supposed to be the same as currentLocalClockMs
            newLocalClockMs = hvcTimestamp.elementAt(nodeId).getTimeValueInMs();
        }

        // update elements that you have not heard of for a long time
        for(int i = 0; i < getSize(); i++){
            if(hvcTimestamp.elementAt(i).getTimeValueInMs() < newLocalClockMs - epsilon) {
                hvcTimestamp.elementAt(i).setTimeValueInMs(newLocalClockMs - epsilon);
            }
        }


        // Duong debug
//        System.out.println(" old localClock " + currentLocalClock + " new localClock " + newLocalClock);
//        System.out.println();
//        System.out.println(" ++ After increment: ");
//        System.out.println(toStringHvcFull(0));

    }

    /**
     * @param hvc1 first input HVC
     * @param hvc2 second input HVC
     * @return Return a new HVC timestamp whose elements are the max of both HVCs' timestamp
     *         or null if some error happens
     */
    public static Vector<HvcElement> getMaxHvcTimestamp(HVC hvc1, HVC hvc2){
        if(hvc1 == null || hvc2 == null) {
            System.out.println("HVC.getMaxHvc: Error. At least one of the HVCs is null");

            return null;
        }

        int numOfElements = hvc1.getHvcTimestamp().size();

        if(numOfElements != hvc2.getHvcTimestamp().size()) {
            System.out.println("HVC.getMaxHvc: Error. Size of two HVCs does not match");

            return null;
        }

        Vector<HvcElement> result = new Vector<HvcElement>(numOfElements);

        for(int i = 0; i < numOfElements; i++){
            long timeValue = Math.max( hvc1.getHvcTimestamp().elementAt(i).getTimeValue(),
                                        hvc2.getHvcTimestamp().elementAt(i).getTimeValue());
            HvcElement hvcElement = new HvcElement();
            hvcElement.setTimeValue(timeValue);

            result.addElement(hvcElement);

        }

        return result;
    }


    /**
     * Should be used by server only
     * Display every information of Hvc
     * @return
     */
    public synchronized String toStringHvcFull(int indent){
        String indentStr = new String(new char[indent]).replace("\0", " ");

        StringBuilder result = new StringBuilder();

        result.append(  indentStr + "HVC:" + "\n" +
                        indentStr + "  nodeId = " + nodeId + "\n" +
                        indentStr + "  activeSize = " + activeSize + "\n" +
                        indentStr + "  epsilon = " + epsilon + "\n" +
                        indentStr + "  hvcTimestamp: " + "\n");

        for(int i = 0; i < hvcTimestamp.size(); i++){
            if(i == nodeId)
                result.append(indentStr + "    * ");
            else
                result.append(indentStr + "      ");

//            String timeStampStr = String.format("%,d%n", hvcTimestamp.elementAt(i));
//            result.append(timeStampStr);
            result.append(hvcTimestamp.elementAt(i).toString(0));
        }

        return result.toString();
    }

    /**
     * Should be used by client only
     * Display only timestamp of HVC
     * @return
     */
    public synchronized String toStringHvcTimestamp(){
        StringBuilder result = new StringBuilder();

        result.append( " HVC:" + "\n" +
                "   hvcTimestamp: " + "\n");
        for(int i = 0; i < hvcTimestamp.size(); i++){
            result.append("      " + hvcTimestamp.elementAt(i).toString(0));
        }

        return result.toString();
    }

    public String toString(){
        return toStringHvcFull(0);
    }

}
