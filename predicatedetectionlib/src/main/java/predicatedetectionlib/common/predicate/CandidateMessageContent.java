package predicatedetectionlib.common.predicate;

import predicatedetectionlib.common.ByteUtils;
import predicatedetectionlib.common.LocalSnapshot;

import java.nio.ByteBuffer;

/**
 * Content of a candidate sent from servers to monitors
 * In old experiment, each monitor is responsible for exactly one predicate, in that case
 * CandidateMessageContent is LocalSnaphshot
 *
 * In experiments with graph coloring, since the number of candidates is large, one monitor
 * has to take care of multiple predicates. In order to differentiate the predicates received,
 * the message need to include a special string uniquely identify the candidate
 * Created by duongnn on 3/19/18.
 */
public class CandidateMessageContent {
    private String predicateName;
    private LocalSnapshot localSnapshot;

    public CandidateMessageContent(String predicateStr, LocalSnapshot localSnapshot){
        this.predicateName = predicateStr;
        this.localSnapshot = localSnapshot;
    }

    // empty constructor
    // used with fromBytes method to construct an actual object
    public CandidateMessageContent(){}

    public String getPredicateName() {
        return predicateName;
    }

    public void setPredicateName(String predicateName) {
        this.predicateName = predicateName;
    }

    public LocalSnapshot getLocalSnapshot() {
        return localSnapshot;
    }

    public void setLocalSnapshot(LocalSnapshot localSnapshot) {
        this.localSnapshot = localSnapshot;
    }

    // construct CandidateMessageContent from byte array
    // this method goes along with the empty constructor
    // return number of bytes actually read
    public int fromBytes(byte[] byteArray, int offset){
        int originalOffset = offset;

        // read the number of bytes representing this CandidteMessageContent
        int cmcLength = ByteUtils.readInt(byteArray, offset);
        offset += ByteUtils.SIZE_OF_INT;

        // read predicateName length and content
        int predicateNameLength = ByteUtils.readInt(byteArray, offset);
        offset += ByteUtils.SIZE_OF_INT;

        // Duong debug
        if(predicateNameLength > 100) {
            System.out.println(" predicateNameLength = " + predicateNameLength);
        }

        byte[] predicateNameContent = new byte[predicateNameLength];
        System.arraycopy(byteArray, offset, predicateNameContent, 0, predicateNameLength);
        offset += predicateNameLength;
        this.predicateName = new String(predicateNameContent);

        LocalSnapshot localSnapshot = new LocalSnapshot();
        int localSnapshotSize = localSnapshot.fromBytes(byteArray, offset);
        offset += localSnapshotSize;
        this.localSnapshot = localSnapshot;

        return offset - originalOffset;

    }

    // serial into byte array
    //

    /**
     * Serialize a CandidateMessageContent into a byte array.
     * Beside returning the byte array, it also return the actual size of that byte array
     * through the parameter
     * @param actualSize actualSize[0] is actualSize of returned byte array
     * @return byte array of CandidateMessageContent, ready for transmission
     */
    public byte[] toBytes(int[] actualSize){
        // the first 4 bytes will be reserved for the size of this candidateMessageContent
        int byteArraySize = ByteUtils.SIZE_OF_INT + sizeInBytes();

        byte[] byteArray = new byte[byteArraySize];
        int offset = 0;

        // at first write 0 for size
        ByteUtils.writeInt(byteArray, 0, offset);
        offset += ByteUtils.SIZE_OF_INT;

        // write out size and content of predicateName
        ByteUtils.writeInt(byteArray, predicateName.length(), offset);
        offset += ByteUtils.SIZE_OF_INT;
        System.arraycopy(predicateName.getBytes(), 0, byteArray, offset, predicateName.length());
        offset += predicateName.length();

        // write out localSnapshot
        int[] localSnapshotLength = new int[1];
        byte[] localSnapshotByteArray = localSnapshot.toBytes(localSnapshotLength);

        System.arraycopy(localSnapshotByteArray, 0, byteArray, offset, localSnapshotLength[0]);
        //offset += localSnapshot.sizeInBytes();
        offset += localSnapshotLength[0];

        // re-write value of size at offset 0
        actualSize[0] = offset;
        ByteUtils.writeInt(byteArray, offset, 0);

        return byteArray;

    }

    public int sizeInBytes(){
        return ByteUtils.SIZE_OF_INT +              // length of predicateName
                predicateName.length() +    // content of predicateName
                localSnapshot.sizeInBytes();
    }

    public String toString(int indent){
        String indentStr
                = new String(new char[indent]).replace("\0", " ");

        String result = String.format("%sCandidateMessageContent: %n", indentStr) +
                String.format("%s  predicateName : %s %n", indentStr, predicateName) +
                localSnapshot.toString(indent + 2);

        return result;
    }

    public String toString(){
        return toString(0);
    }

    /**
     * Probe a given byte buffer if it contains a complete CandidateMessageContent
     * @param byteBuffer
     * @return true if byte buffer contains a complete CandidateMessageContent
     *         false if otherwise
     */
    public static boolean probeByteBuffer(ByteBuffer byteBuffer){
        byte[] backingArray = byteBuffer.array();
        int offset = byteBuffer.position();

        // probe CMC length and read it
        int cmcLength;
        if(offset + ByteUtils.SIZE_OF_INT > byteBuffer.remaining()){
            return false;
        }else{
            cmcLength = ByteUtils.readInt(backingArray, offset);
            offset += ByteUtils.SIZE_OF_INT;
            byteBuffer.position(offset);
        }

        // probe predName length and read it
        int predNameLength;
        if(offset + ByteUtils.SIZE_OF_INT > byteBuffer.remaining()) {
            return false;
        }else {
            predNameLength = ByteUtils.readInt(backingArray, offset);
            offset += ByteUtils.SIZE_OF_INT;
            byteBuffer.position(offset);
        }

        // probe predName and skip over it
        if(offset + predNameLength > byteBuffer.remaining()) {
            return false;
        }else {
            // skip over predName
            offset += predNameLength;
            byteBuffer.position(offset);
        }

        // proble local snapshot
        return LocalSnapshot.probeByteBuffer(byteBuffer);
    }

    /**
     * Quick probe a given byte buffer if it contains a complete CandidateMessageContent
     * @param byteBuffer
     * @return true if byte buffer contains a complete CandidateMessageContent
     *         false if otherwise
     */
    public static boolean quickProbeByteBuffer(ByteBuffer byteBuffer){
        byte[] backingArray = byteBuffer.array();
        int offset = byteBuffer.position();

        // probe CMC length and read it
        int cmcLength;
        if(offset + ByteUtils.SIZE_OF_INT > byteBuffer.remaining()){
            return false;
        }else{
            cmcLength = ByteUtils.readInt(backingArray, offset);
            offset += ByteUtils.SIZE_OF_INT;
            byteBuffer.position(offset);
        }

        if(offset + cmcLength > byteBuffer.remaining()){
            return false;
        }else{
            return true;
        }

    }

}
