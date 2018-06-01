package predicatedetectionlib.common;

/**
 * Created by duongnn on 6/15/17.
 */
public class LocalSnapshotContentSizeValues {
    int numberOfRows;
    int sizeOfStringsInBytes;

    public LocalSnapshotContentSizeValues(int numberOfRows, int sizeOfStringsInBytes){
        this.numberOfRows = numberOfRows;
        this.sizeOfStringsInBytes = sizeOfStringsInBytes;
    }

    public int getNumberOfRows(){
        return numberOfRows;
    }

    public int getSizeOfStringsInBytes(){
        return sizeOfStringsInBytes;
    }

    public void setNumberOfRows(int numberOfRows){
        this.numberOfRows = numberOfRows;
    }

    public void setSizeOfStringsInBytes(int sizeOfStringsInBytes){
        this.sizeOfStringsInBytes = sizeOfStringsInBytes;
    }

}
