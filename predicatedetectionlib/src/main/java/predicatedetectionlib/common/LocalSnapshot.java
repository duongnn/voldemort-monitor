package predicatedetectionlib.common;

import com.google.common.collect.Table;
import predicatedetectionlib.versioning.MonitorVectorClock;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Snapshot of a local process
 * Should contain:
 *      The vector clock (HVC) when the snapshot is taken
 *      An unique ID for easy search. We could use HVC as ID too
 *      Content of local state. With Voldemort, it is the values of variables
 *          Content of local state at this time just be the mapping of variable names and values
 *          In future, to reduce the time and space, we could use logging like Retroscope
 *
 * Created by duongnn on 5/23/17.
 */

public class LocalSnapshot implements Serializable{
    public static final int MAX_NUMBER_OF_PROCESSES = 50;

    private long sequenceNumber;
    private HvcInterval hvcInterval;
    private LocalSnapshotContent snapshotContent;
    private long receivedTime;  // time when the localsnapshot is received at monitor


    public LocalSnapshot(HvcInterval hvcInterval, long seqNumber, Table<String, MonitorVectorClock, String> content){
        this(hvcInterval, seqNumber, new LocalSnapshotContent(content));

    }

    public LocalSnapshot(HvcInterval hvcInterval, long seqNumber, LocalSnapshotContent content){
        this.hvcInterval = hvcInterval;
        this.sequenceNumber = seqNumber;
        this.snapshotContent = content;

    }

    // Dump local snapshot
    // It is used with method readFromByteArray
    public LocalSnapshot(){
    }

    // this method is used to provide the initial localsnapshot
    // in global state
    public static LocalSnapshot createPlaceHolderLocalSnapshot(int size, int nodeId){
        long sequenceNumber = 0;
        LocalSnapshotContent snapshotContent = null;
        long physicalTime = 1005L; // a randomly chosen number
        HVC startPoint = new HVC(size, 1000L, nodeId, physicalTime);
        HvcInterval hvcInterval = new HvcInterval(physicalTime, startPoint, startPoint);
        return new LocalSnapshot(hvcInterval, sequenceNumber, snapshotContent);
    }

    public long getSequenceNumber(){
        return sequenceNumber;
    }

    public HvcInterval getHvcInterval(){
        return hvcInterval;
    }


    public int getProcessId(){
        return hvcInterval.getStartPoint().getNodeId();
    }

    public LocalSnapshotContent getSnapshotContent(){
        return snapshotContent;
    }

    public long getReceivedTime(){
        return receivedTime;
    }

    public void setReceivedTime(long receivedTime){
        this.receivedTime = receivedTime;
    }

    // construct LocalSnapshot from byte array
    // this method goes along with the empty constructor
    // return number of bytes actually read
    public int fromBytes(byte[] byteArray, int offset){
        int numberOfBytes;
        int originalOffset = offset;

        // read sequence number
        long seqNumber = ByteUtils.readLong(byteArray, offset);
        offset += ByteUtils.SIZE_OF_LONG;

        // read interval
        HvcInterval hvcInterval = new HvcInterval();
        numberOfBytes = hvcInterval.readFromByteArray(byteArray, offset);
        offset += numberOfBytes;

        // read local snapshot content
        LocalSnapshotContent lsc = new LocalSnapshotContent();
        numberOfBytes = lsc.readFromByteArray(byteArray, offset);
        offset += numberOfBytes;

        // construct this local snapshot
        this.hvcInterval = hvcInterval;
        this.sequenceNumber = seqNumber;
        this.snapshotContent = lsc;

        return offset - originalOffset;
    }

    // serialize LocalSnapshot into byte array
    public byte[] toBytes(int[] actualLength){
        int byteArraySize = sizeInBytes();

        //CommonUtils.log(" byteArraySize = " + byteArraySize);

        byte[] byteArray = new byte[byteArraySize];
        int offset = 0;
        int numberOfBytes;

        // write out sequenceNumber
        ByteUtils.writeLong(byteArray, sequenceNumber, offset);
        offset += ByteUtils.SIZE_OF_LONG;

        // write out HVC interval
        numberOfBytes = hvcInterval.writeToByteArray(byteArray, offset);
        offset += numberOfBytes;

        // write out local snapshot content
        numberOfBytes = snapshotContent.writeToByteArray(byteArray, offset);
        offset += numberOfBytes;

        actualLength[0] = offset;

        return byteArray;
    }

    public int sizeInBytes(){
        return  ByteUtils.SIZE_OF_LONG                    // sequenceNumber
                + hvcInterval.getSizeInByteArray()        // HVC interval
                + snapshotContent.getSizeInByteArray();   // local snapshot content
    }

    // display the content of local snapshot
    public String toString(int indent){
        String indentStr
                = new String(new char[indent]).replace("\0", " ");

        String result = String.format("%sLocal snapshot: %n", indentStr) +
        String.format("%s  seq. num.     : %d %n", indentStr, sequenceNumber) +
        String.format("%s  received time : %,d %n", indentStr, getReceivedTime()) +
        hvcInterval.toString(indent + 2) +
        snapshotContent.toString(indent + 2);

        return result;
    }

    public String toString(){
        return toString(0);
    }

    public static boolean probeByteBuffer(ByteBuffer byteBuffer){
        byte[] backingArray = byteBuffer.array();
        int offset = byteBuffer.position();

        // probe sequence number and skip it
        if(offset + ByteUtils.SIZE_OF_LONG > byteBuffer.remaining()) {
            return false;
        }else {
            offset += ByteUtils.SIZE_OF_LONG;
            byteBuffer.position(offset);
        }

        // probe HVC interval
        if(! HvcInterval.probeByteBuffer(byteBuffer))
            return false;

        // probe local snapshot content
        return LocalSnapshotContent.probeByteBuffer(byteBuffer);

    }
}

