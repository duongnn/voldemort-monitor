//package predicatedetectionlib.monitor;
//
//import predicatedetectionlib.common.CommonUtils;
//import predicatedetectionlib.common.LocalSnapshot;
//import java.util.Vector;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//
///**
// * Created by duongnn on 6/21/17.
// */
///**
// *  consumer process
// *  It will read localSnapshot placed in the shared blocking queue by the producer
// *  This process also run the centralized conjunctive predicate detection
// */
//
//public class SinglePredicateCandidateProcessingLinear extends SinglePredicateCandidateProcessing {
//
//    SinglePredicateCandidateProcessingLinear(Vector<LinkedBlockingQueue<LocalSnapshot>> aQueueArray){
//        super(aQueueArray);
//    }
//
//    public void run(){
//        try {
//
////            while (true) {
//            while (!SinglePredicateMonitor.stopThreadNow) {
//
//                // get next red process
//                int redProc;
//
//                while ((redProc = globalSnapshot.getNextRedProcess()) != -1) {
//                    LocalSnapshot nextCandidate;
//
//                    // retrieve candidate from the corresponding queue
//                    // wait if needed
//                    // nextCandidate = candidateQueueArray.elementAt(redProc).take();
//
//                    while (true) {
//                        nextCandidate = candidateQueueArray.elementAt(redProc).poll(100L, TimeUnit.SECONDS);
//                        if (nextCandidate != null)
//                            break;
//                    }
//
//
//                    // replace current candidate in global snapshot by next candidate
//                    globalSnapshot.replaceCandidate(nextCandidate, redProc);
//
//                    // update color after the replacement
//                    globalSnapshot.updateColorAfterReplaceCandidate(redProc);
//
//                }
//
//                globalSnapshot.setDetectionTime(System.currentTimeMillis());
//                globalSnapshot.computeResponseTime();
//
//                // at this time, all candidates are green, i.e. globalSnapshot is consistent
//
//                // write to file for debugging
////                globalSnapshot.outputToFile();
//
//                System.out.println("\n ***** GLOBAL CONJUNCTIVE PREDICATE DETECTED: \n");
////                System.out.println(globalSnapshot.toString());
////                for(int procCount = 0; procCount < getNumberOfProcesses(); procCount ++){
////                    System.out.println("         seq = " + globalSnapshot.getProcessCandidate(procCount).getSequenceNumber());
////                }
//
//
//                SinglePredicateMonitor.historyResponseTime.addElement(globalSnapshot.getResponseTime());
//                SinglePredicateMonitor.historyPhysicalResponseTime.addElement(globalSnapshot.getPhysicalResponseTime());
//
//                globalSnapshot.restart();
//            }
//
//            // Duong debug
//            System.out.println("SinglePredicateCandidateProcessingLinear thread ended");
//
//
//        }catch (InterruptedException e) {
//            CommonUtils.log(e.getMessage());
//            //CommonUtils.log(" Get interruption. Exit ... ");
//            //System.exit(1);
//        }
//
//    }
//}
