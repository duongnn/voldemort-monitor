//package predicatedetectionlib.monitor;
//
//import predicatedetectionlib.common.LocalSnapshot;
//
//import java.util.Vector;
//import java.util.concurrent.LinkedBlockingQueue;
//
///**
// * Created by duongnn on 11/15/17.
// */
//public abstract class SinglePredicateCandidateProcessing implements Runnable{
//    // arriving candidates from servers will queue up in those queue
//    // each queue for one server/process
//    // these queued are shared with producer process, thus it need to be initialize
//    // by main function
//    protected final Vector<LinkedBlockingQueue<LocalSnapshot>> candidateQueueArray;
//    protected GlobalSnapshot globalSnapshot;
//
//    SinglePredicateCandidateProcessing(Vector<LinkedBlockingQueue<LocalSnapshot>> aQueueArray){
//        candidateQueueArray = aQueueArray;
//        globalSnapshot = new GlobalSnapshot(getNumberOfProcesses());
//        SinglePredicateMonitor.historyResponseTime = new Vector<>();
//        SinglePredicateMonitor.historyPhysicalResponseTime = new Vector<>();
//    }
//
//    public int getNumberOfProcesses(){
//        return candidateQueueArray.size();
//    }
//
//    // subclass will implement this function differently
//    public abstract void run();
//}
