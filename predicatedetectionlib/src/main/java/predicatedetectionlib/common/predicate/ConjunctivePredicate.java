package predicatedetectionlib.common.predicate;

import com.google.common.collect.Table;
import predicatedetectionlib.versioning.MonitorVectorClock;

import java.util.Vector;

/**
 * Created by duongnn on 4/1/18.
 */
public class ConjunctivePredicate extends Predicate {

    public ConjunctivePredicate(int predicateId, String predicateName, String monitorIpAddr, String monitorPortNumber, PredicateType type, Vector<ConjunctiveClause> conjClauseList){
        super(predicateId, predicateName, monitorIpAddr, monitorPortNumber, type, conjClauseList);
    }

    /**
     * Check if a GraphEdgePredicate is active w.r.t. the current data at server.
     * A conjunctive predicate is inactive at a process when its local predicate is false.
     * In key-value store server, this can be approximated as the evaluation of predicate on
     * server database (predVarCache) to be false.
     * This is just an approximation since predVarCache is not local state of process because
     * the server stores states of all processes.
     *
     * So for safe, may be we just return true
     *
     * @param dataSet
     * @return true, for safe
     */
    public boolean isPredicateActiveAtServer(Table<String, MonitorVectorClock, String> dataSet){
        return true;
    }
}
