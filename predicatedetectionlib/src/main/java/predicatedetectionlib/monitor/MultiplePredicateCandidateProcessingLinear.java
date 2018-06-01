package predicatedetectionlib.monitor;

import predicatedetectionlib.common.LocalSnapshot;
import predicatedetectionlib.common.predicate.Predicate;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by duongnn on 4/2/18.
 */
public class MultiplePredicateCandidateProcessingLinear {
    public static void runOneDetectionStepLinear(
            Predicate pred,
            ConcurrentHashMap<String, Vector<InternalBuffer>> predicateInternalBufferMap,
            ConcurrentHashMap<String, Vector<LinkedBlockingQueue<LocalSnapshot>>> predicateCandidateQueueArrayMap,
            ConcurrentHashMap<String, GlobalSnapshot> predicateGlobalSnapshotMap,
            ConcurrentHashMap<String, Predicate> monitorPredicateList,
            ConcurrentHashMap<String, Long> predicateStatus){

        // todo
    }

}
