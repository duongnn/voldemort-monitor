package predicatedetectionlib.monitor;

import predicatedetectionlib.common.LocalSnapshot;
import predicatedetectionlib.common.predicate.Predicate;

import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static predicatedetectionlib.common.CommonUtils.*;
import static predicatedetectionlib.common.HVC.INFINITY_TIMESTAMP;
import static predicatedetectionlib.monitor.MultiplePredicateCandidateProcessingLinear.runOneDetectionStepLinear;
import static predicatedetectionlib.monitor.MultiplePredicateCandidateProcessingSemilinear.runOneDetectionStepSemilinear;
import static predicatedetectionlib.monitor.MultiplePredicateMonitor.*;

/**
 * Created by duongnn on 3/27/18.
 */
public class MultiplePredicateCandidateProcessing implements Runnable {
    ConcurrentHashMap<String, Vector<InternalBuffer>> predicateInternalBufferMap;
    ConcurrentHashMap<String, Vector<LinkedBlockingQueue<LocalSnapshot>>> predicateCandidateQueueArrayMap;
    ConcurrentHashMap<String, GlobalSnapshot> predicateGlobalSnapshotMap;
    ConcurrentHashMap<String, Predicate> monitorPredicateList;
    ConcurrentHashMap<String, Long> monitorPredicateStatus;

    MultiplePredicateCandidateProcessing(
            ConcurrentHashMap<String, Vector<InternalBuffer>> predicateInternalBufferMap,
            ConcurrentHashMap<String, Vector<LinkedBlockingQueue<LocalSnapshot>>> predicateCandidateQueueArrayMap,
            ConcurrentHashMap<String, GlobalSnapshot> predicateGlobalSnapshotMap,
            ConcurrentHashMap<String, Predicate> monitorPredicateList,
            ConcurrentHashMap<String, Long> monitorPredicateStatus){

        this.predicateInternalBufferMap = predicateInternalBufferMap;
        this.predicateGlobalSnapshotMap = predicateGlobalSnapshotMap;
        this.predicateCandidateQueueArrayMap = predicateCandidateQueueArrayMap;
        this.monitorPredicateList = monitorPredicateList;
        this.monitorPredicateStatus = monitorPredicateStatus;

    }

    public void run(){
        while(!Monitor.stopThreadNow){

            // Duong debug
            if(monitorPredicateList.size() > maxMonitorPredicateListSize){
                maxMonitorPredicateListSize = monitorPredicateList.size();
            }

            // Duong debug
            maxMonitorPredicateStatusSize = monitorPredicateStatus.size();

            // for each candidate in monitorPredicateList
            //    process it
            for(Predicate pred : monitorPredicateList.values()){
                processPredicate(pred);
            }

        }

    }

    void processPredicate(Predicate pred){
        // to phases
        // 1. processing
        // 2. cleaning

        // processing, no need of exclusive lock
        // get status of predicate
        String predName = pred.getPredicateName();
        int predStatus = getPredicateStatusAtMonitor(predName, monitorPredicateStatus,
                MultiplePredicateMonitor.getPredicateExpirationTime(),
                predicateInternalBufferMap,
                predicateCandidateQueueArrayMap);

        switch(predStatus){

            case MONITOR_PREDICATE_STATUS_ACTIVE:

            case MONITOR_PREDICATE_STATUS_INACTIVE:
                // run one detection step
                runOneDetectionStep(pred);
                break;

            case MONITOR_PREDICATE_STATUS_EXPIRED:
                break;

            default:
                // just skip
        }

        // cleaning
        // we should obtain exclusive access
        synchronized (sharedLockForReceiverAndProcessor){
            predStatus = getPredicateStatusAtMonitor(predName, monitorPredicateStatus,
                    MultiplePredicateMonitor.getPredicateExpirationTime(),
                    predicateInternalBufferMap,
                    predicateCandidateQueueArrayMap);

            switch(predStatus){

                case MONITOR_PREDICATE_STATUS_ACTIVE:

                case MONITOR_PREDICATE_STATUS_INACTIVE:
                    // update status of predicate
                    if(changePredicateStatusToInactive(pred)){
                        MultiplePredicateMonitor.changePredicateStatusToInactiveSuccess ++;
                    }else{
                        MultiplePredicateMonitor.changePredicateStatusToInactiveFail ++;
                    }

                    break;

                case MONITOR_PREDICATE_STATUS_EXPIRED:
                    // clean the data structure if it has not been cleaned
                    cleanPredicateDataStructures(pred);

                    break;

                default:
                    // just skip
            }
        }

    }

    void runOneDetectionStep(Predicate pred){
        switch(pred.getType()){
            case LINEAR:
                runOneDetectionStepLinear(pred, predicateInternalBufferMap, predicateCandidateQueueArrayMap, predicateGlobalSnapshotMap, monitorPredicateList, monitorPredicateStatus);
                break;

            case SEMILINEAR:
                runOneDetectionStepSemilinear(pred, predicateInternalBufferMap, predicateCandidateQueueArrayMap, predicateGlobalSnapshotMap, monitorPredicateList, monitorPredicateStatus);
                break;
        }

    }

    /**
     * Change status of predicate to inactive if appropriate
     * A predicate will be changed to inactive when:
     *     Candidates in global snapshot indicate that predicate is inactive
     *          how candidates indicate inactivity depends on type of predicate
     *     internalBuffer for predicate is empty
     *     candidateQueueArray for predicate is empty
     *
     * If inactive, monitorPredicateStatus will have value != 0L
     * @param pred
     * @return true if predicate is changed from active to inactive
     *         false otherwise
     */
    boolean changePredicateStatusToInactive(Predicate pred){

        if(monitorPredicateStatus.get(pred.getPredicateName()) != 0L) {
            // Duong debug
            MultiplePredicateMonitor.changePredicateStatusToInactiveFailAlreadyInactive ++;

            // predicate already inactive
            return false;
        }

        // check if candidates indicate inactivity
        if(!doesCandidatesShowPredInactive(pred, predicateGlobalSnapshotMap.get(pred.getPredicateName()))){

            // Duong debug
            MultiplePredicateMonitor.changePredicateStatusToInactiveFailCandidate ++;

            return false;
        }

        // check if internal buffer empty
        Vector<InternalBuffer> predInternalBuffer = predicateInternalBufferMap.get(pred.getPredicateName());
        for(int procId = 0; procId < predInternalBuffer.size(); procId ++){
            if(! predInternalBuffer.elementAt(procId).getUnderlyingVector().isEmpty()){

                // Duong debug
                MultiplePredicateMonitor.changePredicateStatusToInactiveFailInternalBuffer ++;

                return false;
            }
        }

        // check if candidate queue array is empty
        Vector<LinkedBlockingQueue<LocalSnapshot>> predCandidateQueueArray = predicateCandidateQueueArrayMap.get(pred.getPredicateName());
        for(int procId = 0; procId < predCandidateQueueArray.size(); procId ++){
            if(! predCandidateQueueArray.elementAt(procId).isEmpty()){

                // Duong debug
                MultiplePredicateMonitor.changePredicateStatusToInactiveFailQueueArray ++;

                return false;
            }
        }

        // all conditions are met
        // mark as inactive
        markPredicateStatusInactive(pred.getPredicateName(), monitorPredicateStatus);

        return true;
    }

    /**
     * Check if candidates in global snapshot for a predicate indicate that predicate is inactive
     * @param pred
     * @param globalSnapshot
     * @return  true if candidates indicate inactivity
     *          false otherwise
     */
    boolean doesCandidatesShowPredInactive(Predicate pred, GlobalSnapshot globalSnapshot){
        switch (pred.getType()){
            case LINEAR:
                // any candidate has infinite endpoint is sufficient
                for(int procId = 0; procId < globalSnapshot.getNumberOfProcesses(); procId ++){
                    if(globalSnapshot.getProcessCandidate(procId).getHvcInterval().getEndPoint().isInfinityHVC()) {
                        return true;
                    }
                }

                return false;

            case SEMILINEAR:
                // all candidates need to have infinite endpoints
                for(int procId = 0; procId < globalSnapshot.getNumberOfProcesses(); procId ++){
                    if(!globalSnapshot.getProcessCandidate(procId).getHvcInterval().getEndPoint().isInfinityHVC()) {
                        return false;
                    }
                }

                return true;

            default:
                System.out.println(" doesCandidatesShowPredInactive: ERROR: unknow predicate type " + pred.getType());
                return false;
        }
    }

    // assume appropriate locks have been obtained
    void cleanPredicateDataStructures(Predicate pred){
        String predName = pred.getPredicateName();

        // internal buffer
        if (predicateInternalBufferMap.containsKey(predName)) {
            predicateInternalBufferMap.remove(predName);
        }

        // candidate queue array
        if (predicateCandidateQueueArrayMap.containsKey(predName))
            predicateCandidateQueueArrayMap.remove(predName);

        // predicate list
        if (monitorPredicateList.containsKey(predName))
            monitorPredicateList.remove(predName);

        // global snapshot
        if (predicateGlobalSnapshotMap.containsKey(predName))
            predicateGlobalSnapshotMap.remove(predName);

    }



    /**
     * try to make a global snapshot consistent (i.e. its candidate concurrent)
     * by advancing it with new candidates from queues
     * @param gs
     * @param candidateQueueArray
     * @return true if gs is consistent or consistent after advancing
     *         false if gs is inconsistent and we cannot advance gs any further (i.e. at least
     *         one process is red and it has no more candidate)
     */
    static boolean tryMakeGlobalSnapshotConcurrentWithoutBlocking(GlobalSnapshot gs, Vector<LinkedBlockingQueue<LocalSnapshot>> candidateQueueArray){
        int redProc;
        while((redProc = gs.getNextRedProcess()) != -1){
            // retrieve next candidate for process redProc

            // no more candidate
            if(candidateQueueArray.elementAt(redProc).peek() == null){
                return false;
            }

            LocalSnapshot nextCandidate = candidateQueueArray.elementAt(redProc).poll();

            // replace candidate
            gs.replaceCandidate(nextCandidate, redProc);

            // update color after replacement
            gs.updateColorAfterReplaceCandidate(redProc);
        }

        return true;
    }

}
