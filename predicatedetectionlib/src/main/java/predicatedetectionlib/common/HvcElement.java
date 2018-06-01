package predicatedetectionlib.common;

/**
 * Created by duongnn on 10/5/17.
 */
public class HvcElement {
    static final long RESOLUTION_FACTOR = 1000000; // 10^3: microsecond; 10^6: nanosecond

    // this will be physical time in milliseconds multiplied by 10^6
    // so we have nanosecond resolution. We need such virtual nano-second
    // resolution because HVC assumes there is time tick between every operations,
    // otherwise HVC could grow far ahead of physical time.
    // On the other hand, we could not use System.nanoTime() since the returned value
    // is not accurate, in the sense that the origin of time could be arbitrary
    private long timeValue;

    // this constructor will create an instance of HvcElement
    // value of timeValue will be set by either setTimeValue() or setTimeValueMs()
    public HvcElement(){
    }

//    // the constructor takes in physical time in nanoseconds
//    public HvcElement(long timeValue){
//        this.timeValue = timeValue;
//    }

    public static int getHvcElementSize(){
        return ByteUtils.SIZE_OF_LONG;
    }

    public long getTimeValue() {
        return timeValue;
    }

    public void setTimeValue(long timeValue) {
        this.timeValue = timeValue;
    }

    public long getTimeValueInMs(){
        return timeValue / RESOLUTION_FACTOR;
    }

    public void setTimeValueInMs(long timeInMs){
        timeValue = timeInMs * RESOLUTION_FACTOR;
    }

    public String toString(int indent){
        String indentStr = new String(new char[indent]).replace("\0", " ");

        return String.format("%s %,d %n", indentStr, timeValue);

    }


}

