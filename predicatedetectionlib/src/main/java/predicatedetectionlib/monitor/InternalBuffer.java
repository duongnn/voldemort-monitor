package predicatedetectionlib.monitor;

import predicatedetectionlib.common.CommonUtils;
import predicatedetectionlib.common.LocalSnapshot;

import java.util.Map;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by duongnn on 6/21/17.
 */
public class InternalBuffer{
    private final long missingCandidateTimeOut; // if missing candidate has not been received by that time, we can believe it is missing

    private Vector<LocalSnapshot> underlyingVector;
    private long expectedSequenceNumber;    // the sequence number you are waiting for so that you can deliver the buffer
                                            // assume server generate sequence number starting from 1

    InternalBuffer(){
        underlyingVector = new Vector<>();
        expectedSequenceNumber = 1L;
        missingCandidateTimeOut = 2000L;
    }

    public Vector<LocalSnapshot> getUnderlyingVector(){
        return underlyingVector;
    }

    public long getExpectedSequenceNumber(){
        return expectedSequenceNumber;
    }

    public void jumpExpectedSequenceNumber(long someSeqNum){
        expectedSequenceNumber = someSeqNum;
    }

    public void incrementExpectedSequenceNumber(){
        expectedSequenceNumber ++;
    }

    /**
     * insert a local snapshot into internal buffer
     * then if the local snapshots in the buffer are ready to deliver (i.e. no missing local snapshot),
     * deliver it to the candidate queue
     * @param predname name of predicate
     * @param localSnapshot local snapshot to be inserted
     * @param candidateQueue candidate queue to deliver when ready
     */
    public void insertAndDeliverLocalSnapshot(String predname, LocalSnapshot localSnapshot, LinkedBlockingQueue<LocalSnapshot> candidateQueue){

//        // Duong debug
//        System.out.println(" pred: " + predname + ":insert LS into CQA with seq. " + localSnapshot.getSequenceNumber() +
//                " expecting " + getExpectedSequenceNumber());


        // if the packet has expired, ignore
        if(getExpectedSequenceNumber() > localSnapshot.getSequenceNumber()){
            return;
        }

        if(underlyingVector.isEmpty()){
            underlyingVector.addElement(localSnapshot);
        }else{
            // find your position to insert
            long newSequenceNumber = localSnapshot.getSequenceNumber();
            int currentPosition = 0;
            long currentSequenceNumber = underlyingVector.elementAt(currentPosition).getSequenceNumber();
            while(currentSequenceNumber < newSequenceNumber){
                currentPosition ++;
                if(currentPosition >= underlyingVector.size()) {
                    // reach end of vector
                    break;
                }

                currentSequenceNumber = underlyingVector.elementAt(currentPosition).getSequenceNumber();
            }

            // insert element at currentPosition
            // it should shift elements with higher seq. number
            underlyingVector.insertElementAt(localSnapshot, currentPosition);
        }

        flushInternalBuffer(predname, candidateQueue);

    }


    /**
     * if the heading of the internal buffer is ready, deliver them, i.e. put them into the specified queue
     * @param candidateQueue
     */
    private void flushInternalBuffer(String predName, LinkedBlockingQueue<LocalSnapshot> candidateQueue){
        // check if the first element sequence number match with the one expected
        if(underlyingVector.isEmpty())
            return;

        LocalSnapshot firstLocalSnapshot = underlyingVector.firstElement();
        long firstSequenceNumber = firstLocalSnapshot.getSequenceNumber();

//        // Duong debug
//        CommonUtils.log("  Pred: " + predName + " proc: " + firstLocalSnapshot.getProcessId() +
//                " firstSeq = " + firstSequenceNumber +
//                " expectSeq = " + getExpectedSequenceNumber());

        // in case some sequence numbers are missing, we need to check whether it is on the way
        // or it has been lost
        if(firstSequenceNumber > getExpectedSequenceNumber()){
            // how long the first one has been waiting?
            long firstLocalSnapshotWaitTime = System.currentTimeMillis() - firstLocalSnapshot.getReceivedTime();

            // if that waiting time exceed timeout, the sequence numbers before it are likely
            // be lost, we should not wait for those any more
            if(firstLocalSnapshotWaitTime > missingCandidateTimeOut){
                jumpExpectedSequenceNumber(firstSequenceNumber);
            }
        }

        while(firstSequenceNumber == getExpectedSequenceNumber()){
            // put the first element into the queue
            if(candidateQueue.offer(firstLocalSnapshot) == true){

//                // Duong debug
//                CommonUtils.log("     ++ Pred: " + predName +
//                        "proc " + firstLocalSnapshot.getProcessId() +
//                        " seq " + firstSequenceNumber +
//                        " inserted into CQA");

            }else{

//                // Duong debug
//                CommonUtils.log("     ++ Pred: " + predName +
//                        "proc " + firstLocalSnapshot.getProcessId() +
//                        ": CQA is full");

                // just quit and try in another time
                break;
            }

            // remove first from the vector
            underlyingVector.removeElementAt(0);

            // increment expected sequence number
            incrementExpectedSequenceNumber();

            // if buffer is empty, no more flushing
            if(underlyingVector.isEmpty())
                break;

            // move to the next element if possible (i.e. buffer is not empty yet)
            firstLocalSnapshot = underlyingVector.firstElement();
            firstSequenceNumber = firstLocalSnapshot.getSequenceNumber();
        }

    }

}
