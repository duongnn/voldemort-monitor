package predicatedetectionlib.common;

import com.google.common.io.ByteStreams;
import predicatedetectionlib.monitor.Monitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Vector;

/**
 * Created by duongnn on 6/19/17.
 */
public class HvcInterval {

    public static final long INFINITY_EPSILON = Long.MAX_VALUE;

    private long physicalStartTime; // physical time of startPoint

    private HVC startPoint;
    private HVC endPoint;

    // the concept of intervalLength is not universal enough
    // i.e. some intervals can be represented by startPoint + intervalLength
    // but some are not. The universal way is startPoint + endPoint
    //    private long intervalLength;

    // this empty constructor will go with the method readFromByteArray()
    public HvcInterval(){}

    public HvcInterval(long physicalStartTime, HVC startPoint, HVC endPoint){
        this.startPoint = startPoint;
        this.endPoint = endPoint;
        this.physicalStartTime = physicalStartTime;
    }

    public HVC getStartPoint(){
        return startPoint;
    }

    public HVC getEndPoint(){
        return endPoint;
    }

    public int getNumberOfEntries(){
        return startPoint.getSize();
    }

    public void setStartPoint(HVC startPoint){
        this.startPoint = startPoint;
    }

    public void setEndPoint(HVC endPoint){
        this.endPoint = endPoint;
    }

    public long getPhysicalStartTime() {
        return physicalStartTime;
    }

    public void setPhysicalStartTime(long physicalStartTime) {
        this.physicalStartTime = physicalStartTime;
    }

    /**
     * Comparison between 2 intervals
     * Extended from comparison between 2 HVCs
     * @param they
     * @param twoEpsilon uncertainty window
     * @return
     */

    public int happenBefore(HvcInterval they, long twoEpsilon){
        /*
            + Use VC standard

              If interval1.endpoint < interval2.startpoint
                  return interval1 -> interval2

              If interval2.endpoint < interval1.startpoint
                  return interval2 -> interval1

            + now we have interval1 concurrent to interval 2 in VC metrics
              we will use HVC metrics

              if interval1.endpoint.primaryElement < interval2.startpoint.primaryElement - someEpsilon
                return interval1 -> interval2

              If interval2.endpoint.primaryElement < interval1.startpoint.primaryElement - someEpsilon
                return interval2 -> interval1

              // they are concurrent in both VC and HVC metrics
              return interval2 concurrent interval1
        */

        // we have to construct endpoints
        HVC myStartPoint = this.getStartPoint();
        HVC myEndPoint = this.getEndPoint();
        HVC theyStartPoint = they.getStartPoint();
        HVC theyEndPoint = they.getEndPoint();

        if(myEndPoint.happenBefore(theyStartPoint, twoEpsilon) == HVC.BEFORE)
            return HVC.BEFORE;

        if(theyEndPoint.happenBefore(myStartPoint, twoEpsilon) == HVC.BEFORE)
            return HVC.AFTER;

        // now two intervals are concurrent in VC metrics
        // consider HVC metrics
        if(myEndPoint.getPrimaryElement().getTimeValueInMs() < theyStartPoint.getPrimaryElement().getTimeValueInMs() - twoEpsilon)
            return HVC.BEFORE;

        if(theyEndPoint.getPrimaryElement().getTimeValueInMs() < myStartPoint.getPrimaryElement().getTimeValueInMs() - twoEpsilon)
            return HVC.AFTER;

        // two intervals are concurrent in VC and HVC metrics
        return HVC.CONCURRENT;
    }

    public synchronized String toString(int indent){
        String indentStr = new String(new char[indent]).replace("\0", " ");

        StringBuilder result = new StringBuilder();

        result.append( indentStr + "HVC Interval:" + "\n" +
//                        indentStr + "  physical start time: " + physicalStartTime + "\n" +
                        String.format("%s  physical start time: %,d %n", indentStr, physicalStartTime) +
                        indentStr + "  Start point: \n" +
                        startPoint.toStringHvcFull(indent + 2) +
                        indentStr + "  End point  : \n" +
                        endPoint.toStringHvcFull(indent + 2));

        return result.toString();
    }

    public int getSizeInByteArray(){
        // Duong comment: the HVC that server sends to monitor does not need to include
        //                information like activeSize (since it could be recalculated)
        //                or epsilon (since monitor will use its own epsilon)

        int numberOfHvcEntries = getNumberOfEntries();

        return  ByteUtils.SIZE_OF_INT // startPoint/endPoint HVC nodeId
                + ByteUtils.SIZE_OF_INT // startPoint/endPoint number of timestamp entries
                + ByteUtils.SIZE_OF_LONG // for physicalStartTime
                + ByteUtils.SIZE_OF_LONG * numberOfHvcEntries  // startPoint HVC timestamp vector
                + ByteUtils.SIZE_OF_LONG * numberOfHvcEntries; // endPoint HVC timestamp vector

    }

    // write this HVC interval into byte array for transmission
    // return number of byte written or 0 if error
    public int writeToByteArray(byte[] byteArray, int offset){
        // sanity check
        if(getSizeInByteArray() + offset > byteArray.length){
            System.out.println(" HvcInterval.writeToByteArray(): ERROR: not enough space in byte array");
            return 0;   // no byte written
        }

        int originalOffset = offset;
        int numberOfHvcEntries = getNumberOfEntries();

        // added: write physicalStartTime
        ByteUtils.writeLong(byteArray, physicalStartTime, offset);
        offset += ByteUtils.SIZE_OF_LONG;

        // first write the node id
        ByteUtils.writeInt(byteArray, startPoint.getNodeId(), offset);
        offset += ByteUtils.SIZE_OF_INT;

        // second: write the number of timestamp entries
        ByteUtils.writeInt(byteArray, numberOfHvcEntries, offset);
        offset += ByteUtils.SIZE_OF_INT;

        // write out array of hvc timestamp of starting point
        for(int i = 0; i < numberOfHvcEntries; i++){
            ByteUtils.writeLong(byteArray, startPoint.getHvcTimestamp().elementAt(i).getTimeValue(), offset);
            offset += ByteUtils.SIZE_OF_LONG;
        }

        // write out array of hvc timestamp of ending point
        for(int i = 0; i < numberOfHvcEntries; i++){
            ByteUtils.writeLong(byteArray, endPoint.getHvcTimestamp().elementAt(i).getTimeValue(), offset);
            offset += ByteUtils.SIZE_OF_LONG;
        }

        return offset - originalOffset;

    }

    // read content of an interval from byteArray
    // return number of bytes read
    public int readFromByteArray(byte[] byteArray, int offset){
        int originalOffset = offset;

        // added: read physicalStartTime first
        this.physicalStartTime = ByteUtils.readLong(byteArray, offset);
        offset += ByteUtils.SIZE_OF_LONG;

        // first read node id
        int nodeId = ByteUtils.readInt(byteArray, offset);
        offset += ByteUtils.SIZE_OF_INT;

        // next read number of entries
        int numberOfHvcEntries = ByteUtils.readInt(byteArray, offset);
        offset += ByteUtils.SIZE_OF_INT;

        // Duong debug
        if(numberOfHvcEntries > 10) {
            System.out.println(" numberOfHvcEntries = " + numberOfHvcEntries);
        }

        // just discard this number
        try {
            ByteStreams.nullOutputStream().write(numberOfHvcEntries);
        }catch(IOException ioe){
            // Do nothing
        }


        // then read array of starting point timestamp
        Vector<HvcElement> startPointTimeStampVector = new Vector<>(numberOfHvcEntries);
        for(int i = 0; i < numberOfHvcEntries; i++){
            HvcElement hvcElement = new HvcElement();
            hvcElement.setTimeValue(ByteUtils.readLong(byteArray, offset));
            startPointTimeStampVector.addElement(hvcElement);
            offset += ByteUtils.SIZE_OF_LONG;
        }

        // then read array of ending point timestap
        Vector<HvcElement> endPointTimeStampVector = new Vector<>(numberOfHvcEntries);
        for(int i = 0; i < numberOfHvcEntries; i++){
            HvcElement hvcElement = new HvcElement();
            hvcElement.setTimeValue(ByteUtils.readLong(byteArray, offset));
            endPointTimeStampVector.addElement(hvcElement);
            offset += ByteUtils.SIZE_OF_LONG;
        }

        // construct interval
        this.startPoint = new HVC(nodeId, Monitor.getEpsilon(), startPointTimeStampVector);
        this.endPoint = new HVC(nodeId, Monitor.getEpsilon(), endPointTimeStampVector);

        return offset - originalOffset;
    }

    /**
     * probe if a byte buffer contains a complete HvcInterval
     * A pseudo copy of readFromByteArray
     * @param byteBuffer
     * @return true if byte buffer contains a complete HvcInterval
     *         false otherwise
     */
    public static boolean probeByteBuffer(ByteBuffer byteBuffer){
        byte[] backingArray = byteBuffer.array();
        int offset = byteBuffer.position();

        // probe physical start time and skip over it
        if(offset + ByteUtils.SIZE_OF_LONG > byteBuffer.remaining()) {
            return false;
        }else{
            offset += ByteUtils.SIZE_OF_LONG;
            byteBuffer.position(offset);
        }

        // probe node id and skip over it
        if(offset + ByteUtils.SIZE_OF_INT > byteBuffer.remaining()){
            return false;
        }else{
            offset += ByteUtils.SIZE_OF_INT;
            byteBuffer.position(offset);
        }

        // probe and read number of Hvc entries
        int numberOfHvcEntries;
        if(offset + ByteUtils.SIZE_OF_INT > byteBuffer.remaining()){
            return false;
        }else{
            numberOfHvcEntries = ByteUtils.readInt(backingArray, offset);
            offset += ByteUtils.SIZE_OF_INT;
            byteBuffer.position(offset);
        }

        // skip over two arrays of timestamp
        if(offset + 2 * numberOfHvcEntries * ByteUtils.SIZE_OF_LONG > byteBuffer.remaining()) {
            return false;
        }else{
            offset += 2 * numberOfHvcEntries * ByteUtils.SIZE_OF_LONG;
        }

        return true;

    }

}
