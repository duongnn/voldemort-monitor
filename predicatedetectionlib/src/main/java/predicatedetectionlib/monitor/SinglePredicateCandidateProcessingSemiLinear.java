//package predicatedetectionlib.monitor;
//
//import predicatedetectionlib.common.CommonUtils;
//import predicatedetectionlib.common.HVC;
//import predicatedetectionlib.common.HvcInterval;
//import predicatedetectionlib.common.LocalSnapshot;
//import predicatedetectionlib.common.predicate.ConjunctiveClause;
//import predicatedetectionlib.common.predicate.Predicate;
//
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.Vector;
//import java.util.concurrent.LinkedBlockingQueue;
//
///**
// * Created by duongnn on 11/15/17.
// */
//public class SinglePredicateCandidateProcessingSemiLinear extends SinglePredicateCandidateProcessing {
//    SinglePredicateCandidateProcessingSemiLinear(Vector<LinkedBlockingQueue<LocalSnapshot>> aQueueArray){
//        super(aQueueArray);
//    }
//
//    public void run(){
//        try {
//            System.out.println(" SinglePredicateCandidateProcessingSemiLinear");
//
////            while (true) {
//            while (!SinglePredicateMonitor.stopThreadNow) {
//
//                HashMap<Integer, Vector<Integer>> globalActiveClauses =
//                        getListOfGlobalActiveConjClauses(globalSnapshot, SinglePredicateMonitor.getMonitorPredicate());
//
//                // size of globalActiveClauses is also the number of critical section being used
//                // if > 1, mutual exclusion is violated
//                while(globalActiveClauses.size() <= 1){
//                    // we need to advance global snapshot with a candidate
//
//                    // get set of eligible states
//                    Vector<LocalSnapshot> eligibleSet = getEligibleSetHVC();
//
//                    if(eligibleSet.size() == 0){
//                        // could not find eligible state using HVC, we have to advance it using VC
//                        advanceGlobalSnapshotWithVC(globalSnapshot);
//
//                    }else{
//                        if(eligibleSet.size() == 1){
//                            // update global snapshot with this only eligible candidate
//                            LocalSnapshot semiForbiddenCandidate = eligibleSet.elementAt(0);
//                            globalSnapshot.replaceCandidate(semiForbiddenCandidate, semiForbiddenCandidate.getProcessId());
//
//                            // remove candidate from the queue, since we just peek() it, not poll it
//                            if(! candidateQueueArray.elementAt(semiForbiddenCandidate.getProcessId()).remove(semiForbiddenCandidate)){
//                                System.out.println(" SinglePredicateCandidateProcessingSemiLinear ERROR: could not remove element from linked blocking queue");
//                                Thread.dumpStack();
//                                System.exit(1);
//                            }
//                        }else{
//                            // eligibleSet.size() > 1
//                            // choose the local state that will not increase the number of CS being used
//                            // (i.e. the number of active conjunctive clauses)
//                            boolean predicateIsDetected = false;
//
//                            LinkedList<LocalSnapshot> secondOptions = new LinkedList<>();
//
//                            for(LocalSnapshot ls : eligibleSet) {
//                                // first, get the list of conjunctive clauses active in this local state
//                                Vector<Integer> localActiveClauses = getListOfLocalActiveConjClauses(ls, SinglePredicateMonitor.getMonitorPredicate());
//
//                                // if > 1. If greedy, we can report predicate detected now
//                                // In Garg algorithm, we avoid candidate where CS(s) is true. It is because
//                                // in Garg algorithm, the candidates are directly from processes, not via servers.
//                                // Therefore, in that candidate, at most one process (process that send the candidate)
//                                // is using CS.
//                                // In Voldemort, the candidate is state of server, and server could capture state of multiple
//                                // clients, thus number of active clauses could be > 1
//                                if(localActiveClauses.size() > 1){
//                                    // this is a clear violation of mutual exclusion
//                                    globalSnapshot.replaceCandidate(ls, ls.getProcessId());
//
//                                    // remove candidate from the queue
//                                    if(! candidateQueueArray.elementAt(ls.getProcessId()).remove(ls)){
//                                        System.out.println(" SinglePredicateCandidateProcessingSemiLinear ERROR: could not remove element from linked blocking queue");
//                                        Thread.dumpStack();
//                                        System.exit(1);
//                                    }
//
//                                    // no need to update globalActiveClauses
//
//                                    predicateIsDetected = true;
//                                    break;
//                                }else if(localActiveClauses.size() == 1){
//                                    // if = 1: check if this clause is already in the hashmap globalActiveClauses
//                                    //        if no: and if globalActiveClauses is not empty,
//                                    //               then mutual exclusion violation is detected too
//                                    //        if yes: put in secondOptions list
//                                    if((globalActiveClauses.size() == 1) &&
//                                       (!globalActiveClauses.containsKey(localActiveClauses.elementAt(0)))){
//                                        globalSnapshot.replaceCandidate(ls, ls.getProcessId());
//
//                                        // remove candidate from the queue
//                                        if(! candidateQueueArray.elementAt(ls.getProcessId()).remove(ls)){
//                                            System.out.println(" SinglePredicateCandidateProcessingSemiLinear ERROR: could not remove element from linked blocking queue");
//                                            Thread.dumpStack();
//                                            System.exit(1);
//                                        }
//
//                                        // no need to update globalActiveClauses
//
//                                        predicateIsDetected = true;
//                                        break;
//                                    }else{
//                                        // put on secondOptions list
//                                        secondOptions.addLast(ls);
//                                    }
//                                }else{
//                                    // if = 0: put in waiting secondOptions list
//                                    secondOptions.addFirst(ls);
//                                }
//
//                            }
//
//                            if(predicateIsDetected)
//                                break; // break the while(globalActiveClauses.size() <= 1) loop
//
//                            LocalSnapshot guessedSemiForbidden = secondOptions.getFirst();
//                            globalSnapshot.replaceCandidate(guessedSemiForbidden, guessedSemiForbidden.getProcessId());
//
//                            // remove candidate from the queue
//                            if(! candidateQueueArray.elementAt(guessedSemiForbidden.getProcessId()).remove(guessedSemiForbidden)){
//                                System.out.println(" SinglePredicateCandidateProcessingSemiLinear ERROR: could not remove element from linked blocking queue");
//                                Thread.dumpStack();
//                                System.exit(1);
//                            }
//                        }
//                    }
//
//                    globalActiveClauses = getListOfGlobalActiveConjClauses(globalSnapshot, SinglePredicateMonitor.getMonitorPredicate());
//                }
//
//                // When we get to this point, a globalSnapshot with mutual exclusion violation is found
//                // Report it
//
//                globalSnapshot.setDetectionTime(System.currentTimeMillis());
//                globalSnapshot.computeResponseTime();
//
//                // write to file for debugging
//                globalSnapshot.outputToFile();
//
//                System.out.println("\n ***** GLOBAL MUTUAL EXCLUSION VIOLATION DETECTED: \n");
////                System.out.println(globalSnapshot.toString());
////                for(int procCount = 0; procCount < getNumberOfProcesses(); procCount ++){
////                    System.out.println("         seq = " + globalSnapshot.getProcessCandidate(procCount).getSequenceNumber());
////                }
//
//
//                SinglePredicateMonitor.historyResponseTime.addElement(globalSnapshot.getResponseTime());
//                SinglePredicateMonitor.historyPhysicalResponseTime.addElement(globalSnapshot.getPhysicalResponseTime());
//
//                // move on to find next cases
//
//                // advance global snapshot along local states where there is active clause(s)
//                invalidateActiveLocalStates(globalSnapshot);
//                makeGlobalSnapshotConsistent(globalSnapshot);
//
//            }
//
//            // Duong debug
//            System.out.println("SinglePredicateCandidateProcessingSemiLinear thread ended");
//
//        }catch (Exception e) {
//            CommonUtils.log(e.getMessage());
//        }
//    }
//
//    /**
//     * Get the list of local processes in which some conjClause being true/active
//     * in a given global snapshot
//     *
//     * @param semiPred : mutual exclusion predicate, which is a list of conjunctive clauses
//     *                   that should not be mutually true together
//     * @param gs : global snapshot
//     * @return A hashmap that maps
//     *              id of each process to the
//     *              list of ids of conjunctive clauses being active at that process
//     */
//    HashMap<Integer, Vector<Integer>> getListOfActiveProcesses(GlobalSnapshot gs, Predicate semiPred){
//
//        HashMap<Integer, Vector<Integer>> activeProcesses = new HashMap<>();
//
//        for(int pid = 0; pid < gs.getNumberOfProcesses(); pid ++){
//            LocalSnapshot localSnapshot = gs.getProcessCandidate(pid);
//            Vector<Integer> activeClauses = getListOfLocalActiveConjClauses(localSnapshot, semiPred);
//
//            // only add process with active conjunctive clauses
//            if(activeClauses.size() > 0){
//                activeProcesses.put(pid, activeClauses);
//            }
//        }
//
//        return activeProcesses;
//    }
//
//    /**
//     * Get the list of conjClause being true/active
//     * in a given global snapshot
//     *
//     * This is the counterpart of getListOfActiveProcesses
//     *
//     * Note that there is a difference in predicate evaluation between server and monitor
//     *   At server, predicate is locally true when any of conjClause is true
//     *   At monitor, for mutual exclusion, global predicate is true when more than 1 conjClause is true
//     *
//     * If the number of conjClause being true > 0, mutual exclusion is violated
//     *
//     * @param semiPred : mutual exclusion predicate, which is a list of conjunctive clauses
//     *                   that should not be mutually true together
//     * @param gs : global snapshot
//     * @return A hashmap that maps
//     *              id of each conjunctive clause that is active to the
//     *              list of process ids at which that clause is active
//     */
//
//    HashMap<Integer, Vector<Integer>> getListOfGlobalActiveConjClauses(GlobalSnapshot gs, Predicate semiPred){
//        HashMap<Integer, Vector<Integer>> activeClauses = new HashMap<>();
//
//        for(ConjunctiveClause conjClause : semiPred.getConjunctiveClauseList()){
//            Vector<Integer> activeProcesses = new Vector<>();
//
//            for(int pid = 0; pid < gs.getNumberOfProcesses(); pid++){
//                LocalSnapshot localSnapshot = gs.getProcessCandidate(pid);
//                if(conjClause.evaluateClause(localSnapshot.getSnapshotContent()) == true){
//                    activeProcesses.addElement(pid);
//                }
//            }
//
//            // only add active clauses
//            if(activeProcesses.size() > 0){
//                activeClauses.put(conjClause.getClauseId(), activeProcesses);
//            }
//        }
//
//        return activeClauses;
//    }
//
//    /**
//     * Get list of conjunctive clauses active at a local snapshot
//     * @param localSnapshot
//     * @param semiPred
//     * @return
//     */
//
//    Vector<Integer> getListOfLocalActiveConjClauses(LocalSnapshot localSnapshot, Predicate semiPred){
//        Vector<Integer> activeClauses = new Vector<>();
//        for(ConjunctiveClause conjClause : semiPred.getConjunctiveClauseList()){
//                if(conjClause.evaluateClause(localSnapshot.getSnapshotContent()) == true){
//                    activeClauses.addElement(conjClause.getClauseId());
//                }
//        }
//        return activeClauses;
//    }
//
//
//
//    /**
//     * @return set of eligible states, i.e. states that could be added to globalSnapshot without
//     *         causing HVC consistency
//     *         empty if no such state found
//     */
//    Vector<LocalSnapshot> getEligibleSetHVC(){
//        Vector<LocalSnapshot> results = new Vector<>();
//
//        for(int candidatePid = 0; candidatePid < getNumberOfProcesses(); candidatePid++){
//            LocalSnapshot candidate;
//            // busy loop if needed
//            // this will make the detection time increase, because we have to make sure all processes have
//            // next candidate ready
//            while((candidate = candidateQueueArray.elementAt(candidatePid).peek()) == null);
//
//            // check if this candidate is eligible local state
//            boolean isEligible = true;
//            for(int otherPid = 0; otherPid < getNumberOfProcesses(); otherPid ++){
//                // skip itself
//                if(otherPid == candidatePid)
//                    continue;
//
//                HvcInterval otherHvcInterval = globalSnapshot.getProcessCandidate(otherPid).getHvcInterval();
//                if(candidate.getHvcInterval().happenBefore(otherHvcInterval, SinglePredicateMonitor.getEpsilon()*2) != HVC.CONCURRENT){
//                    isEligible = false;
//                    break;
//                }
//            }
//            if(isEligible){
//                results.addElement(candidate);
//            }
//        }
//
//        return results;
//
//    }
//
//    /**
//     * @return set of eligible states according to VC consistency criteria
//     *         should not return null
//     *         in case the queues are empty, just wait
//     * Note that unlike getEligibleSetHVC where we should check != HVC.CONCURRENT,
//     * in this case, candidate is almost sure will be AFTER.
//     * The reason is that, in HVC, clocks are automatically computed if not heard for a long time, thus if it is
//     * AFTER in HVC criteria, it will also AFTER in VC (actually we do not have VC information,
//     * we just have HVC).
//     * Therefore, any new candidate from the queue should be accepted
//     */
//    Vector<LocalSnapshot> getEligibleSetVC(){
//
//        Vector<LocalSnapshot> results = new Vector<>();
//
//        for(int candidatePid = 0; candidatePid < getNumberOfProcesses(); candidatePid++){
//            LocalSnapshot candidate;
//            // busy loop if needed
//            // this will make the detection time increase, because we have to make sure all processes have
//            // next candidate ready
//            while((candidate = candidateQueueArray.elementAt(candidatePid).peek()) == null);
//
//            results.addElement(candidate);
//        }
//        return results;
//    }
//
//    /**
//     * This function is called when we could not advance global snapshot using HVC
//     * @param gs global snapshot
//     */
//    void advanceGlobalSnapshotWithVC(GlobalSnapshot gs){
//        Vector<LocalSnapshot> eligibleSetVc = getEligibleSetVC();
//
//        if(eligibleSetVc.size() == 0){
//            System.out.println(" advanceGlobalSnapshotWithVC ERROR: could not find eligible candidate in VC criteria");
//            Thread.dumpStack();
//            System.exit(1);
//
//            // indeed, for this case, just select any next candidate from any queue
//            // or more exactly, getEligibleSetVC should return candidates from all queues
//
//        }else{
//            LocalSnapshot candidate = eligibleSetVc.elementAt(0);
//            gs.replaceCandidate(candidate, candidate.getProcessId());
//            gs.updateColorAfterReplaceCandidate(candidate.getProcessId());
//
//            // remove candidate from linked blocking queue
//            if(! candidateQueueArray.elementAt(candidate.getProcessId()).remove(candidate)){
//                System.out.println(" advanceGlobalSnapshotWithVC ERROR: could not remove element from linked blocking queue");
//                Thread.dumpStack();
//                System.exit(1);
//            }
//
//            // After adding candidate, consistency is not maintained. We need to make it consistent
//            makeGlobalSnapshotConsistent(gs);
//
//        }
//    }
//
//    /**
//     * make global snapshot consistent, i.e. make sure all local snapshot are concurrent in HVC criteria
//     * @param gs : global snapshot
//     */
//
//    void makeGlobalSnapshotConsistent(GlobalSnapshot gs){
//
//        // get next red process
//        int redProc;
//
//        while ((redProc = gs.getNextRedProcess()) != -1) {
//            LocalSnapshot nextCandidate;
//
//            // retrieve candidate from the corresponding queue
//            // wait if needed
//            while(true) {
//                try {
//                    nextCandidate = candidateQueueArray.elementAt(redProc).take();
//                    break;
//                }catch(InterruptedException ie){
//                    continue;
//                }
//            }
//
//            // replace current candidate in global snapshot by next candidate
//            gs.replaceCandidate(nextCandidate, redProc);
//
//            // update color after the replacement
//            gs.updateColorAfterReplaceCandidate(redProc);
//
//        }
//    }
//
//
//    /**
//     * mark any local state of a global snapshot as red if that local state
//     * contains any active conjunctive clauses
//     * @param gs
//     */
//    void invalidateActiveLocalStates(GlobalSnapshot gs){
//        HashMap<Integer, Vector<Integer>> activeLocalStates = getListOfActiveProcesses(gs, SinglePredicateMonitor.getMonitorPredicate());
//
//        for(int i : activeLocalStates.keySet()){
//            gs.markProcessAsRed(i);
//        }
//    }
//
//}
