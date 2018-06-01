package predicatedetectionlib.monitor;

import predicatedetectionlib.common.HVC;
import predicatedetectionlib.common.HvcInterval;
import predicatedetectionlib.common.LocalSnapshot;
import predicatedetectionlib.common.predicate.ConjunctiveClause;
import predicatedetectionlib.common.predicate.Predicate;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static predicatedetectionlib.monitor.MultiplePredicateCandidateProcessing.tryMakeGlobalSnapshotConcurrentWithoutBlocking;

/**
 * Created by duongnn on 4/2/18.
 */
public class MultiplePredicateCandidateProcessingSemilinear {

    public static void runOneDetectionStepSemilinear(
            Predicate pred,
            ConcurrentHashMap<String, Vector<InternalBuffer>> predicateInternalBufferMap,
            ConcurrentHashMap<String, Vector<LinkedBlockingQueue<LocalSnapshot>>> predicateCandidateQueueArrayMap,
            ConcurrentHashMap<String, GlobalSnapshot> predicateGlobalSnapshotMap,
            ConcurrentHashMap<String, Predicate> monitorPredicateList,
            ConcurrentHashMap<String, Long> monitorPredicateStatus){

        String predName = pred.getPredicateName();

        // extract global snapshot and candidate queue array
        GlobalSnapshot predGS = predicateGlobalSnapshotMap.get(predName);
        Vector<LinkedBlockingQueue<LocalSnapshot>> predCandidateQueueArray = predicateCandidateQueueArrayMap.get(predName);

        // since candidates in global snapshot may not be concurrent,
        // we need to make them concurrent before further processing

        if(! tryMakeGlobalSnapshotConcurrentWithoutBlocking(predGS, predCandidateQueueArray))
            return;

        // candidates in global snapshot for predicate are concurrent now
        HashMap<Integer, Vector<Integer>> globalActiveClauses = getListOfGlobalActiveConjClauses(predGS, pred);

        // size of globalActiveClauses is also the number of critical section being used
        // if > 1, mutual exclusion is violated
        while(globalActiveClauses.size() <= 1){
            // we need to advance global snapshot with a new candidate

            // get eligible set hvc without being blocked
            Vector<LocalSnapshot> eligibleSet = getEligibleSetHvcWithoutBlocking(predGS, predCandidateQueueArray);

            if(eligibleSet.size() == 0){
                // we have to advance global snapshot using VC
                boolean globalSnapshotIsAdvancedToConcurrentNow =
                        advanceGlobalSnapshotWithVcWithoutBlocking(predGS, predCandidateQueueArray);

                if(! globalSnapshotIsAdvancedToConcurrentNow)
                    return;

            }else if(eligibleSet.size() == 1){
                // we have one and only one choice to advance global snapshot
                // update global snapshot with this only eligible candidate
                LocalSnapshot semiForbiddenCandidate = eligibleSet.elementAt(0);
                predGS.replaceCandidate(semiForbiddenCandidate, semiForbiddenCandidate.getProcessId());

                // remove candidate from the queue, since we just peek() it, not poll it
                if(! predCandidateQueueArray.elementAt(semiForbiddenCandidate.getProcessId()).remove(semiForbiddenCandidate)){
                    Thread.dumpStack();
                    MultiplePredicateMonitor.exitDueToError(" runOneDetectionStepSemiLinear ERROR: could not remove element from linked blocking queue");
                    return;
                }

            }else{ // eligibleSet.size() > 1
                // choose the candidate/local state that will not increase the number of CS being used
                // (i.e. the number of active conjunctive clauses)
                boolean predicateIsDetected = false;

                LinkedList<LocalSnapshot> secondOptions = new LinkedList<>();

                for(LocalSnapshot ls : eligibleSet) {
                    // first, get the list of conjunctive clauses active in this local state
                    Vector<Integer> localActiveClauses = getListOfLocalActiveConjClauses(ls, pred);

                    if(localActiveClauses.size() > 1){
                        // when localActiveClauses.size() > 1, as greedy, we can report predicate detected now.
                        //
                        // In Garg algorithm, we avoid candidate where CS(s) is true. It is because
                        // in Garg algorithm, the candidates are directly from processes, not via servers.
                        // Therefore, in that candidate, at most one process (process that send the candidate)
                        // is using CS.
                        //
                        // In Voldemort, the candidate is state of server, and server could capture states of multiple
                        // clients, thus number of active clauses could be > 1
                        //
                        // If that is the case, it is a clear violation of mutual exclusion
                        predGS.replaceCandidate(ls, ls.getProcessId());

                        // remove candidate from the queue
                        if(! predCandidateQueueArray.elementAt(ls.getProcessId()).remove(ls)){
                            Thread.dumpStack();
                            MultiplePredicateMonitor.exitDueToError(" runOneDetectionStepSemiLinear ERROR: could not remove element from linked blocking queue");
                            return;
                        }

                        // no need to update globalActiveClauses
                        // because after predicate is detected, global snapshot will be invalidated
                        // so that we can move on to detect next violation

                        predicateIsDetected = true;

                        // break for loop of ls
                        break;

                    }else if(localActiveClauses.size() == 1){
                        // localActiveClauses.size() = 1:
                        //     check if this clause is already in the hashmap globalActiveClauses
                        //        if no: and if globalActiveClauses is not empty,
                        //               then mutual exclusion violation is detected too
                        //        if yes: put in secondOptions list
                        if((globalActiveClauses.size() == 1) &&
                                (!globalActiveClauses.containsKey(localActiveClauses.elementAt(0)))){

                            predGS.replaceCandidate(ls, ls.getProcessId());

                            // remove candidate from the queue
                            if(! predCandidateQueueArray.elementAt(ls.getProcessId()).remove(ls)){
                                Thread.dumpStack();
                                MultiplePredicateMonitor.exitDueToError(" runOneDetectionStepSemiLinear ERROR: could not remove element from linked blocking queue");
                                return;
                            }

                            // no need to update globalActiveClauses
                            // because after predicate is detected, global snapshot will be invalidated
                            // so that we can move on to detect next violation

                            predicateIsDetected = true;

                            // break for loop of ls
                            break;

                        }else{
                            // put on secondOptions list
                            secondOptions.addLast(ls);
                        }
                    }else{
                        // localActiveClauses.size() == 0
                        // put in waiting secondOptions list
                        secondOptions.addFirst(ls);
                    }
                }

                if(predicateIsDetected)
                    break; // break the while(globalActiveClauses.size() <= 1) loop

                LocalSnapshot guessedSemiForbidden = secondOptions.getFirst();
                predGS.replaceCandidate(guessedSemiForbidden, guessedSemiForbidden.getProcessId());

                // remove candidate from the queue
                if(! predCandidateQueueArray.elementAt(guessedSemiForbidden.getProcessId()).remove(guessedSemiForbidden)){
                    Thread.dumpStack();
                    MultiplePredicateMonitor.exitDueToError(" runOneDetectionStepSemiLinear ERROR: could not remove element from linked blocking queue");
                    return;
                }

            }

            // at this point, global snapshot is concurrent,
            // we can update globalActiveClauses
            globalActiveClauses = getListOfGlobalActiveConjClauses(predGS, pred);

        } // end of while(globalActiveClauses.size() <= 1)

        // at this point, global snapshot is concurrent and globalActiveClauses.size > 1
        // i.e. mutual exclusion violation is detected
        // report the violation

        predGS.setDetectionTime(System.currentTimeMillis());
        predGS.computeResponseTime();

        // write to file for debugging
        predGS.outputToFile();

        System.out.println("\n ***** GLOBAL MUTUAL EXCLUSION VIOLATION DETECTED: \n");

        System.out.println(predGS.toString());
//        for(int procCount = 0; procCount < predGS.getNumberOfProcesses(); procCount ++){
//            System.out.println("         seq = " + predGS.getProcessCandidate(procCount).getSequenceNumber());
//        }


        Monitor.historyResponseTime.addElement(predGS.getResponseTime());
        Monitor.historyPhysicalResponseTime.addElement(predGS.getPhysicalResponseTime());

        // move on to find next cases

        // advance global snapshot along local states where there is active clause(s)
        invalidateActiveLocalStates(predGS, pred);

        // we may repeat the steps in runOneDetectionStepSemilinear immediately
        // however, we choose to stop after a successful detection and move to next predicate

    }

    /**
     * Get the list of local processes of a global snapshot
     * in which some conjunctive clause of a given predicate being true/active
     *
     * @param semiPred : mutual exclusion predicate, which is a list of conjunctive clauses
     *                   that should not be mutually true together
     * @param gs : global snapshot
     * @return A hashmap that maps
     *              id of each process to the
     *              list of ids of conjunctive clauses being active at that process
     */
    private static HashMap<Integer, Vector<Integer>> getListOfActiveProcesses(GlobalSnapshot gs, Predicate semiPred){

        HashMap<Integer, Vector<Integer>> activeProcesses = new HashMap<>();

        for(int pid = 0; pid < gs.getNumberOfProcesses(); pid ++){
            LocalSnapshot localSnapshot = gs.getProcessCandidate(pid);
            Vector<Integer> activeClauses = getListOfLocalActiveConjClauses(localSnapshot, semiPred);

            // only add process with active conjunctive clauses
            if(activeClauses.size() > 0){
                activeProcesses.put(pid, activeClauses);
            }
        }

        return activeProcesses;
    }

    /**
     * Get the list of conjClause being true/active
     * in a given global snapshot
     *
     * This is the counterpart of getListOfActiveProcesses
     *
     * Note that there is a difference in predicate evaluation between server and monitor
     *   At server, predicate is locally true when any of conjClause is true
     *   At monitor, for mutual exclusion, global predicate is true when more than 1 conjClause is true
     *
     * If the number of conjClause being true > 0, mutual exclusion is violated
     *
     * @param semiPred : mutual exclusion predicate, which is a list of conjunctive clauses
     *                   that should not be mutually true together
     * @param gs : global snapshot
     * @return A hashmap that maps
     *              id of each conjunctive clause that is active to the
     *              list of process ids at which that clause is active
     */

    private static HashMap<Integer, Vector<Integer>> getListOfGlobalActiveConjClauses(GlobalSnapshot gs, Predicate semiPred){
        HashMap<Integer, Vector<Integer>> activeClauses = new HashMap<>();

        for(ConjunctiveClause conjClause : semiPred.getConjunctiveClauseList()){
            Vector<Integer> activeProcesses = new Vector<>();

            for(int pid = 0; pid < gs.getNumberOfProcesses(); pid++){
                LocalSnapshot localSnapshot = gs.getProcessCandidate(pid);
                if(conjClause.evaluateClause(localSnapshot.getSnapshotContent()) == true){
                    activeProcesses.addElement(pid);
                }
            }

            // only add active clauses
            if(activeProcesses.size() > 0){
                activeClauses.put(conjClause.getClauseId(), activeProcesses);
            }
        }

        return activeClauses;
    }


    /**
     * Get list of conjunctive clauses active at a local snapshot
     * @param localSnapshot
     * @param semiPred
     * @return
     */

    private static Vector<Integer> getListOfLocalActiveConjClauses(LocalSnapshot localSnapshot, Predicate semiPred){
        Vector<Integer> activeClauses = new Vector<>();
        for(ConjunctiveClause conjClause : semiPred.getConjunctiveClauseList()){
                if(conjClause.evaluateClause(localSnapshot.getSnapshotContent()) == true){
                    activeClauses.addElement(conjClause.getClauseId());
                }
        }
        return activeClauses;
    }

//    /**
//     * This function is called before getEligibleSetHvcWithoutBlocking()
//     * so that getEligibleSetHvcWithoutBlocking will not be blocked
//     *
//     * @param candidateQueueArray
//     * @return return true if all candidate queue has some new candidate
//     */
//    static boolean allProcessesHaveNewCandidate(Vector<LinkedBlockingQueue<LocalSnapshot>> candidateQueueArray){
//        for(int pid = 0; pid < candidateQueueArray.size(); pid ++){
//            if(candidateQueueArray.elementAt(pid).peek() == null)
//                return false;
//        }
//
//        return true;
//    }

    /**
     * obtain set of eligible states for a given global snapshot from the candidate queue array
     * @param globalSnapshot
     * @param candidateQueueArray
     * @return set of eligible states, i.e. states that could be added to globalSnapshot without
     *         causing HVC consistency
     *         empty if no such state found
     */
    static Vector<LocalSnapshot> getEligibleSetHvcWithoutBlocking(GlobalSnapshot globalSnapshot, Vector<LinkedBlockingQueue<LocalSnapshot>> candidateQueueArray){
        Vector<LocalSnapshot> results = new Vector<>();
        int numberOfProcesses = candidateQueueArray.size();

        for(int candidatePid = 0; candidatePid < numberOfProcesses; candidatePid++){
            LocalSnapshot candidate = candidateQueueArray.elementAt(candidatePid).peek();

            // if it is null, just skip
            if(candidate == null)
                continue;

            // check if this candidate is eligible local state
            boolean isEligible = true;
            for(int otherPid = 0; otherPid < numberOfProcesses; otherPid ++){
                // skip itself
                if(otherPid == candidatePid)
                    continue;

                HvcInterval otherHvcInterval = globalSnapshot.getProcessCandidate(otherPid).getHvcInterval();
                if(candidate.getHvcInterval().happenBefore(otherHvcInterval, Monitor.getEpsilon()*2) != HVC.CONCURRENT){
                    isEligible = false;
                    break;
                }
            }
            if(isEligible){
                results.addElement(candidate);
            }
        }

        return results;
    }

    /**
     * @return set of eligible states according to VC consistency criteria
     *         could return null if queues are empty
     *
     * Note that unlike getEligibleSetHvcWithoutBlocking where we should check != HVC.CONCURRENT,
     * in this case, candidate is almost sure will be AFTER.
     * The reason is that, in HVC, clocks are automatically computed if not heard for a long time, thus if it is
     * AFTER in HVC criteria, it will also AFTER in VC (actually we do not have VC information,
     * we just have HVC).
     * Therefore, any new candidate from the queue should be accepted
     */
    static Vector<LocalSnapshot> getEligibleSetVcWithoutBlocking(GlobalSnapshot globalSnapshot, Vector<LinkedBlockingQueue<LocalSnapshot>> candidateQueueArray){

        Vector<LocalSnapshot> results = new Vector<>();

        for(int candidatePid = 0; candidatePid < globalSnapshot.getNumberOfProcesses(); candidatePid++){
            LocalSnapshot candidate = candidateQueueArray.elementAt(candidatePid).peek();

            if(candidate == null){
                continue;
            }

            results.addElement(candidate);
        }
        return results;
    }


    /**
     * This function will retrieve any candidate from the queue (such retrieval will not be blocked)
     * and advance global snapshot along that candidate.
     * Then it will try to make global snapshot concurrent
     * @param globalSnapshot
     * @param candidateQueueArray
     * @return true if we can advance the global snapshot (i.e. global snapshot is changed) and then make it concurrent
     *         false otherwise
     */
    static boolean advanceGlobalSnapshotWithVcWithoutBlocking(
            GlobalSnapshot globalSnapshot,
            Vector<LinkedBlockingQueue<LocalSnapshot>> candidateQueueArray){

        // this call should not be blocked
        Vector<LocalSnapshot> eligibleSetVc = getEligibleSetVcWithoutBlocking(globalSnapshot, candidateQueueArray);

        if(eligibleSetVc.size() == 0){
            // this means all queues are empty, we have no new candidate
            return false;
        }else{
            // pick any candidate from eligibleSetVc and
            //   put into global snapshot
            //   remove from candidate queue array

            LocalSnapshot candidate = eligibleSetVc.elementAt(0);
            globalSnapshot.replaceCandidate(candidate, candidate.getProcessId());
            globalSnapshot.updateColorAfterReplaceCandidate(candidate.getProcessId());

            if(! candidateQueueArray.elementAt(candidate.getProcessId()).remove(candidate)){
                Thread.dumpStack();
                MultiplePredicateMonitor.exitDueToError(" advanceGlobalSnapshotWithVcWithoutBlocking ERROR: could not remove element from linked blocking queue");
                return false;
            }

            // global snapshot may not be concurrent, we need to make it concurrent
            return tryMakeGlobalSnapshotConcurrentWithoutBlocking(globalSnapshot, candidateQueueArray);
        }
    }

    /**
     * mark any local state of a global snapshot as red if that local state
     * contains any active conjunctive clauses of a given predicate
     * @param gs
     * @param pred
     */
    private static void invalidateActiveLocalStates(GlobalSnapshot gs, Predicate pred){
        HashMap<Integer, Vector<Integer>> activeLocalStates = getListOfActiveProcesses(gs, pred);

        for(int i : activeLocalStates.keySet()){
            gs.markProcessAsRed(i);
        }
    }

}
