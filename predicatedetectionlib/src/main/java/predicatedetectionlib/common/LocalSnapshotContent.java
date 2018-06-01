package predicatedetectionlib.common;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Map;
import com.google.common.collect.Table;
import com.google.common.collect.HashBasedTable;
import predicatedetectionlib.common.predicate.Predicate;
import predicatedetectionlib.versioning.MonitorVectorClock;

/**
 * Content of a local snapshot
 * We created a separate class for it so that we could change the implementation
 * later more easily
 * Created by duongnn on 5/28/17.
 */

public class LocalSnapshotContent{
    public static final int MAX_NUMBER_OF_ROWS = 100;
    public static final int MAX_COLUMN_STRING_LENGTH = 100;

    // local state is a mapping of variable names and version and values
    // It has 3 columns: variable name, version, variable value
    private Table<String, MonitorVectorClock, String> localState;

    public LocalSnapshotContent(){
        this.localState = HashBasedTable.create();
    }

    public LocalSnapshotContent(Table<String, MonitorVectorClock, String> localState){
        this.localState = localState;
    }


    // gets and sets
    public Table<String, MonitorVectorClock, String> getLocalState(){
        return localState;
    }

    public void setLocalState(Table<String, MonitorVectorClock, String> localState){
        this.localState = localState;
    }

    // this play the table
    public void display(PrintStream outPrintStream){
        Map<String, Map<MonitorVectorClock, String>> map = localState.rowMap();

        for(String name : map.keySet()){
            Map<MonitorVectorClock, String> verAndValMap = map.get(name);

            for(Map.Entry<MonitorVectorClock, String> verAndVal : verAndValMap.entrySet()){
                String ver = verAndVal.getKey().toString();
                String val = verAndVal.getValue();

                outPrintStream.println(name + " : " + ver + " => " + val);
            }
        }
    }

    // get the size values, useful for computing the number of bytes
    // needed to serialize this local snapshot content
    public LocalSnapshotContentSizeValues getSizeValues(){
        int numberOfRows = 0;
        int sizeOfStringsInBytes = 0;

        Map<String, Map<MonitorVectorClock, String>> map = localState.rowMap();

        for(String name : map.keySet()){
            Map<MonitorVectorClock, String> verAndValMap = map.get(name);

            numberOfRows += verAndValMap.entrySet().size();

            for(Map.Entry<MonitorVectorClock, String> ColumnTwoThree : verAndValMap.entrySet()){
                MonitorVectorClock ver = ColumnTwoThree.getKey();
                String val = ColumnTwoThree.getValue();

                sizeOfStringsInBytes += name.length() + ver.sizeInBytes() + val.length();
            }
        }

        return new LocalSnapshotContentSizeValues(numberOfRows, sizeOfStringsInBytes);

    }

    public int getSizeInByteArray(){
        /*
           structure for serializing snapshot content
             int                 int        byte[]
          +---------+-------+-------------+------+--------+
          |# of rows|  ...  |size_of_col_k| col_k|   ...  |
          +---------+-------+-------------+------+--------+
            main             sub            sub
            header           header         content
                            there are 3 columns each row
         */

        LocalSnapshotContentSizeValues sizeValues = getSizeValues();
        int numberOfRows = sizeValues.getNumberOfRows();
        int sizeOfStringsInBytes = sizeValues.getSizeOfStringsInBytes();

        // Duong debug
        // CommonUtils.log(" numberOfRows = " + numberOfRows + " sizeOfStringsInBytes = " + sizeOfStringsInBytes);

        return  ByteUtils.SIZE_OF_INT // main header
                + 3 * ByteUtils.SIZE_OF_INT * numberOfRows  // 3 sub-headers each row
                + sizeOfStringsInBytes; // sub-contents
    }

    // write content of local snapshot onto a byte array
    // return the number of bytes written
    // return 0 if there is some error (no byte written)

    public int writeToByteArray(byte[] byteArray, int offset){
        // sanity check
        LocalSnapshotContentSizeValues sv = getSizeValues();
        int expectedNumberOfBytesWritten =
                ByteUtils.SIZE_OF_INT       // main header
                + 3 * sv.getNumberOfRows() * ByteUtils.SIZE_OF_INT   // sub-headers
                + sv.getSizeOfStringsInBytes() ;         // sub-contents

        if( expectedNumberOfBytesWritten + offset > byteArray.length){
            System.out.println(" LocalSnapshotContent.writeToByteArray(): ERROR: not enough space in byte array");
            return 0;   // no byte written
        }

        int originalOffset = offset;

        /*
           structure for serializing snapshot content
             int                 int        byte[]
          +---------+-------+-------------+------+--------+
          |# of rows|  ...  |size_of_col_k| col_k|   ...  |
          +---------+-------+-------------+------+--------+
            main             sub            sub
            header           header         content
        */

        // write out main header, i.e. number of row
        ByteUtils.writeInt(byteArray, sv.getNumberOfRows(), offset);
        offset += ByteUtils.SIZE_OF_INT;

        // for each row of the table, write its 3 columns
        // each column include: size + content
        Map<String, Map<MonitorVectorClock, String>> map = localState.rowMap();

        for(String name : map.keySet()){

            String column[] = new String[3];
            column[0] = name;

            Map<MonitorVectorClock, String> verAndValMap = map.get(name);

            for(Map.Entry<MonitorVectorClock, String> verAndVal : verAndValMap.entrySet()){
                MonitorVectorClock ver = verAndVal.getKey();
                String val = verAndVal.getValue();

                // write out var name
                ByteUtils.writeInt(byteArray, name.length(), offset);
                offset += ByteUtils.SIZE_OF_INT;
                System.arraycopy(name.getBytes(), 0, byteArray, offset, name.length());
                offset += name.length();

                // write out variable version
                ByteUtils.writeInt(byteArray, ver.sizeInBytes(), offset);
                offset += ByteUtils.SIZE_OF_INT;
                System.arraycopy(ver.toBytes(), 0, byteArray, offset, ver.sizeInBytes());
                offset += ver.sizeInBytes();

                // write out variable value
                ByteUtils.writeInt(byteArray, val.length(), offset);
                offset += ByteUtils.SIZE_OF_INT;
                System.arraycopy(val.getBytes(), 0, byteArray, offset, val.length());
                offset += val.length();
            }
        }

        return (offset - originalOffset);
    }


    // construct local snapshot content from a byte array
    // return number of bytes read
    public int readFromByteArray(byte[] byteArray, int offset){
        int originalOffset = offset;

        /*
           structure for serializing snapshot content
             int                 int        byte[]
          +---------+-------+-------------+------+--------+
          |# of rows|  ...  |size_of_col_k| col_k|   ...  |
          +---------+-------+-------------+------+--------+
            main             sub            sub
            header           header         content
        */

        // read number of rows
        int numberOfRows = ByteUtils.readInt(byteArray, offset);
        offset += ByteUtils.SIZE_OF_INT;

        // read each row
        for(int row = 0; row < numberOfRows; row ++){
            // read 3 columns of the row
            String colString[] = new String[3];

            String name;
            MonitorVectorClock ver;
            String val;
            int numOfBytes;
            byte[] nameContent;
            byte[] verContent;
            byte[] valContent;

            // read variable name
            numOfBytes = ByteUtils.readInt(byteArray, offset);
            offset += ByteUtils.SIZE_OF_INT;

            // Duong debug
            if(numOfBytes > 150){
                System.out.println(" varName length is " + numOfBytes);
            }

            nameContent = new byte[numOfBytes];
            System.arraycopy(byteArray, offset, nameContent, 0, numOfBytes);
            offset += numOfBytes;
            name = new String(nameContent);

            // read variable version
            numOfBytes = ByteUtils.readInt(byteArray, offset);
            offset += ByteUtils.SIZE_OF_INT;

            // Duong debug
            if(numOfBytes > 150){
                System.out.println(" verContent length is " + numOfBytes);
            }

            verContent = new byte[numOfBytes];
            System.arraycopy(byteArray, offset, verContent, 0, numOfBytes);
            offset += numOfBytes;
            ver = new MonitorVectorClock(verContent);

            // read variable value
            numOfBytes = ByteUtils.readInt(byteArray, offset);
            offset += ByteUtils.SIZE_OF_INT;

            // Duong debug
            if(numOfBytes > 150){
                System.out.println(" valContent length is " + numOfBytes);
            }

            valContent = new byte[numOfBytes];
            System.arraycopy(byteArray, offset, valContent, 0, numOfBytes);
            offset += numOfBytes;
            val = new String(valContent);

            // insert this row into the mapping
            this.localState.put(name, ver, val);
        }

        return offset - originalOffset;
    }

    // display content
    public String toString(int indent){
        String indentStr = new String(new char[indent]).replace("\0", " ");

        StringBuilder result = new StringBuilder();
        Map<String, Map<MonitorVectorClock, String>> map = localState.rowMap();

        int numberOfRows = 0;

        result.append(indentStr + "localState: \n");
        for(String name : map.keySet()){
            Map<MonitorVectorClock, String> verAndValMap = map.get(name);

            for(Map.Entry<MonitorVectorClock, String> verAndVal : verAndValMap.entrySet()){
                MonitorVectorClock ver = verAndVal.getKey();
                String val = verAndVal.getValue();

                result.append(indentStr + "  " + name + " : " + ver.toString() + " => " + val + "\n");

                numberOfRows ++;
            }
        }
        result.append(indentStr + "total " + numberOfRows + " rows\n");

        return result.toString();
    }

    /**
     * @param pred predicate of interest
     * @return return part of this LocalSnapshotContent that is related to the predicate of interest
     *          i.e. we project this LocalSnapshotContent on the predicate variable list
     *          This will save the network packet size for the candidates
     */
    public LocalSnapshotContent projectOnPredicate(Predicate pred){
        Table<String, MonitorVectorClock, String> projectedTable = HashBasedTable.create();

        // scan over pred variable list. For each variable in the list
        // retrieve all rows in this LocalSnapshotContent localState, and put in projected table
        for (Map.Entry<String, String> entry : pred.getVariableList().entrySet()) {
            String varName = entry.getKey();
            // String varValue = entry.getValue();

            Map<MonitorVectorClock, String> verAndValMap = localState.row(varName);
            for(Map.Entry<MonitorVectorClock, String> verAndVal : verAndValMap.entrySet()){
                projectedTable.put(varName, verAndVal.getKey(), verAndVal.getValue());
            }
        }

        return new LocalSnapshotContent(projectedTable);
    }

    /**
     * probe if a byte buffer contains a complete LocalSnapshotContent
     * A pseudo copy of readFromByteArray
     * @param byteBuffer
     * @return true if byte buffer contains a complete LocalSnapshotContent
     *         false otherwise
     */

    public static boolean probeByteBuffer(ByteBuffer byteBuffer){
        byte[] backingArray = byteBuffer.array();
        int offset = byteBuffer.position();

        // probe and read number of rows
        int numberOfRows;
        if(offset + ByteUtils.SIZE_OF_INT > byteBuffer.remaining()){
            return false;
        }else{
            numberOfRows = ByteUtils.readInt(backingArray, offset);
            offset += ByteUtils.SIZE_OF_INT;
            byteBuffer.position(offset);
        }

        // probe row by row
        for(int row = 0; row < numberOfRows; row ++){
            int numOfBytes;

            // repeat the process of probing 3 times:
            // variable name, variable version, variable value
            for(int i = 0; i < 3; i++) {
                // probe and read length
                if (offset + ByteUtils.SIZE_OF_INT > byteBuffer.remaining()) {
                    return false;
                } else {
                    numOfBytes = ByteUtils.readInt(backingArray, offset);
                    offset += ByteUtils.SIZE_OF_INT;
                    byteBuffer.position(offset);
                }

                // probe and skip over content
                if (offset + numOfBytes > byteBuffer.remaining()) {
                    return false;
                } else {
                    offset += numOfBytes;
                    byteBuffer.position(offset);
                }
            }
        }

        return true;
    }

}

