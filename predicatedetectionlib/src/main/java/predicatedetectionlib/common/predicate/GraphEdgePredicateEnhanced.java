package predicatedetectionlib.common.predicate;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import predicatedetectionlib.versioning.MonitorVectorClock;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * Enhanced version of normal GraphEdgePredicate to reduce false positive
 * The different is the clause list
 * Created by duongnn on 4/22/18.
 */
public class GraphEdgePredicateEnhanced extends Predicate {
    // predicate representing an edge, says between node 10 and node 55
    private String firstNode;   // smaller node, i.e. 10
    private String secondNode;  // larger node, i.e. 55

    public GraphEdgePredicateEnhanced(String firstNode, String secondNode, String monitorIpAddr, String monitorPortNum){
        // 1.  setting variables in super class

        // predicateId is the Cantor pairing function of two node
        int firstNodeVal = Integer.valueOf(firstNode);
        int secondNodeVal = Integer.valueOf(secondNode);
        int predId = (firstNodeVal + secondNodeVal)*(firstNodeVal + secondNodeVal + 1)/2 + secondNodeVal;

        // construct conjunctive clauses
        Vector<ConjunctiveClause> clist = new Vector<>(2);

        // first clause is ecsA_B_A = 1
        HashMap<String, String> varList0 = new HashMap<>();
        varList0.put("ecs" + firstNode + "_" + secondNode + "_" + firstNode, "1");
        ConjunctiveClause conjClause0 = new ConjunctiveClause(0, varList0);
        clist.addElement(conjClause0);

        // second clause is ecsA_B_B = 1
        HashMap<String, String> varList1 = new HashMap<>();
        varList1.put("ecs" + firstNode + "_" + secondNode + "_" + secondNode, "1");
        ConjunctiveClause conjClause1 = new ConjunctiveClause(1, varList1);
        clist.addElement(conjClause1);

        // variable list
        HashMap<String, String>  vList = new HashMap<>();
        for(ConjunctiveClause clause : clist){
            vList.putAll(clause.getVariableList());
        }

        setPredicateId(predId);
        setPredicateName(firstNode + "_" + secondNode);
        setMonitorIpAddr(monitorIpAddr);
        setMonitorPortNumber(monitorPortNum);
        setType(PredicateType.SEMILINEAR);
        setConjunctiveClauseList(clist);
        setVariableList(vList);


        // 2. set up variables only in this class
        this.firstNode = firstNode;
        this.secondNode = secondNode;
    }

    public GraphEdgePredicateEnhanced(String edgeStr, String monitorIpAddr, String monitorPortNum){
        // suppose the edge is provided in the form A_B, i.e. "10_55"
        this(edgeStr.substring(0, edgeStr.indexOf('_')),
                edgeStr.substring(edgeStr.indexOf('_') + 1),
                monitorIpAddr,
                monitorPortNum);
    }


    public String getFirstNode() {
        return firstNode;
    }

    public String getSecondNode() {
        return secondNode;
    }

    public String getEdgeString(){
        return firstNode + "_" + secondNode;
    }

    public String getFirstNodeFlag(){
        return "flag" + firstNode + "_" + secondNode + "_" + firstNode;
    }

    public String getSecondNodeFlag(){
        return "flag" + firstNode + "_" + secondNode + "_" + secondNode;
    }

    // overriding parent method
    public Table<String, MonitorVectorClock, String> generateInactiveMapping(){
        Table<String, MonitorVectorClock, String> result = HashBasedTable.create();

        for(Map.Entry<String, String> entry : variableList.entrySet()){
            result.put(entry.getKey(), new MonitorVectorClock(), "0");
        }

        return result;
    }

    /**
     * Check if a GraphEdgePredicate is active w.r.t. the current data at server.
     * A graph edge predicate is active when some client is interested in that edge.
     * This is reflected with some variable flag = 1
     * @param dataSet the data
     * @return true if any flag variable = 1
     *         false if both flag variable != 1, or not initialized yet
     */
    @Override
    public boolean isPredicateActiveAtServer(Table<String, MonitorVectorClock, String> dataSet){
        // check first node flag
        String firstNodeFlag = this.getFirstNodeFlag();
        Map<MonitorVectorClock, String> verAndValMap = dataSet.row(firstNodeFlag);
        for(Map.Entry<MonitorVectorClock, String> verAndVal : verAndValMap.entrySet()){
            if(verAndVal.getValue().equals("1"))
                return true;
        }

        // check second node flag
        String secondNodeFlag = this.getSecondNodeFlag();
        verAndValMap = dataSet.row(secondNodeFlag);
        for(Map.Entry<MonitorVectorClock, String> verAndVal : verAndValMap.entrySet()){
            if(verAndVal.getValue().equals("1"))
                return true;
        }

        return false;
    }

}

