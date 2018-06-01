package predicatedetectionlib.common.predicate;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import predicatedetectionlib.common.LocalSnapshot;
import predicatedetectionlib.common.LocalSnapshotContent;
import predicatedetectionlib.monitor.MultiplePredicateMonitor;
import predicatedetectionlib.versioning.MonitorVectorClock;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import static predicatedetectionlib.common.CommonUtils.getLocalNonLoopbackAddress;

/**
 * Created by duongnn on 11/8/17.
 *
 * Class for predicate
 * Methods
 *  for server
 *  for monitor
 * Data structure (matching the xml file)
 *  id
 *  type of predicate
 *  list of conjunctive clauses
 *  predicate is true if any of conjunctive clauses is true
 */

public abstract class Predicate {
    public static final int PREDICATE_SUBCLASS_UNKNOWN = 0;
    public static final int PREDICATE_SUBCLASS_GRAPH_EDGE = 1;
    public static final int PREDICATE_SUBCLASS_CONJUNCTIVE = 2;

    protected int predicateId;
    // predicateName is unique since it is used as key for hashmap
    protected String predicateName;
    protected String monitorIpAddr;
    protected String monitorPortNumber;

    protected PredicateType type;   // linear or semilinear

    // list of conjunctive clauses
    // note: any predicate can be defined as disjunctive normal form
    protected Vector<ConjunctiveClause> conjunctiveClauseList;

    // mapping of variable names and variable values
    // if variable has the specified value, local predicate will be true
    protected HashMap<String, String> variableList;


    // default constructor, so that we do not have problem in subclass constructor
    protected Predicate(){};

    public Predicate(int predicateId, String predicateName, String monitorIpAddr, String monitorPortNumber, PredicateType type, Vector<ConjunctiveClause> conjClauseList){
        this.predicateId = predicateId;
        this.predicateName = predicateName;
        this.monitorIpAddr = monitorIpAddr;
        this.monitorPortNumber = monitorPortNumber;
        this.type = type;
        this.conjunctiveClauseList = conjClauseList;

        // Aggregate variable list from clauses
        // Note that althought different clauses could have different mapping for the same variables
        // i.e. in clause_1, we have x1=1 while in clause_2 we have x1=0
        // we do not need MultiMap in this case.
        // The variableList just need one mapping (no duplicate), either x1=1 or x1=0
        // That does not cause problem because this mapping is used to construct the predVarCache
        // The purpose of predVarCache is to maintain mapping variable names and current values
        // That mapping will be passed to each predicate to evaluate.
        // At each predicate, the actual desired mapping will be used.
        this.variableList = new HashMap<>();
        for(ConjunctiveClause clause : conjClauseList){
            this.variableList.putAll(clause.getVariableList());
        }
    }

    public int getPredicateId() {
        return predicateId;
    }

    public void setPredicateId(int predicateId) {
        this.predicateId = predicateId;
    }

    public String getPredicateName(){
        return predicateName;
    }

    public void setPredicateName(String predicateName){
        this.predicateName = predicateName;
    }

    public String getMonitorIpAddr() {
        return monitorIpAddr;
    }

    public void setMonitorIpAddr(String monitorIpAddr) {
        this.monitorIpAddr = monitorIpAddr;
    }

    public String getMonitorPortNumber() {
        return monitorPortNumber;
    }

    public void setMonitorPortNumber(String monitorPortNumber) {
        this.monitorPortNumber = monitorPortNumber;
    }

    public PredicateType getType() {
        return type;
    }

    public void setType(PredicateType type) {
        this.type = type;
    }

    public Vector<ConjunctiveClause> getConjunctiveClauseList() {
        return conjunctiveClauseList;
    }

    public void setConjunctiveClauseList(Vector<ConjunctiveClause> conjunctiveClauseList) {
        this.conjunctiveClauseList = conjunctiveClauseList;
    }

    public HashMap<String, String> getVariableList() {
        return variableList;
    }

    protected void setVariableList(HashMap<String, String> variableList) {
        this.variableList = variableList;
    }

    // evaluate local predicate
    // local predicate is true when any of conjunctive clauses is true
    public boolean evaluate(LocalSnapshotContent localSnapshotContent){

//        // Duong debug
//        System.out.println(" evaluating predicateId = " + predicateId);
//        System.out.println(localSnapshotContent.toString(0));
//        System.out.println(" number of conjunctive clauses: " + conjunctiveClauseList.size() + "\n");


        for(ConjunctiveClause clause : conjunctiveClauseList){
            boolean eval =clause.evaluateClause(localSnapshotContent);

//            // Duong debug
//            System.out.println("   clauseId: " + clause.getClauseId() + " => " + eval + "\n");

            if(eval == true)
                return true;
        }

        return false;
    }

    // string representation of predicate
    public String toString(int indent){
        String indentStr = new String(new char[indent]).replace("\0", " ");

        StringBuilder result = new StringBuilder();

        result.append(indentStr + "predicate:\n");
        result.append(indentStr + "  id: " + getPredicateId() + "\n");
        result.append(indentStr + "  predName: " + predicateName + "\n");
        result.append(indentStr + "  monitorAddr: \n");
        result.append(indentStr + "    ip: " + getMonitorIpAddr() + "\n");
        result.append(indentStr + "    port: " + getMonitorPortNumber() + "\n");
        result.append(indentStr + "  type: " + getType() + "\n");
        result.append(indentStr + "  clauseList:\n");
        for(ConjunctiveClause clause : conjunctiveClauseList){
            result.append(clause.toString(indent + 2));
        }

        return result.toString();
    }

    public String toString(){
        return toString(0);
    }


    /**
     * Enhanced version of getRelatedPredicateNameFromKey
     * For example, with GraphEdgePredicate
     *      predicateName: "A_B"
     *      keys:
     *          ecsA_B_A
     *          ecsA_B_B
     * @param keyStr
     * @return
     */
    public static String getRelatedPredicateNameFromKeyEnhanced(String keyStr){
        String result = null;

        // case for GraphEdgePredicate
        // note that testing for substring may not enough
        // we have to make sure the prefix is ecs
        if(keyStr.startsWith("ecs")){
            int endPosition = keyStr.lastIndexOf('_');
            result = keyStr.substring(3, endPosition);
            return result;
        }

        // case for auto-generated linear predicate
        if(keyStr.indexOf("cPred") != -1){
            int endPosition = keyStr.indexOf('_');
            result = keyStr.substring(0, endPosition);
            return result;
        }

        // other type of auto-generated predicates
        // TBD

        // manual predicate
        // TBD

//        // Duong debug
//        System.out.println("getRelatedPredicateNameFromKeyEnhanced: keyStr = " + keyStr + " relatedPredName = " + result);

        return result;

    }

    /**
     * Given a key, identify the name of related predicate
     * For example, with GraphEdgePredicate
     *      predicateName: "A_B"
     *      keys:
     *          flagA_B_A
     *          flagA_B_B
     *          turnA_B
     * @param keyStr
     * @return return string like A_B
     */
    public static String getRelatedPredicateNameFromKey(String keyStr){
        String result = null;

        // case for GraphEdgePredicate
        // note that testing for substring may not enough
        // we have to make sure the prefix is either flag or turn
//        if(keyStr.indexOf("flag") != -1){
        if(keyStr.startsWith("flag")){
            int endPosition = keyStr.lastIndexOf('_');
            result = keyStr.substring(4, endPosition);
        }

//        if(keyStr.indexOf("turn") != -1){
        if(keyStr.startsWith("turn")){
            result = keyStr.substring(4);
        }

        // case for auto-generated linear predicate
        if(keyStr.indexOf("cPred") != -1){
            int endPosition = keyStr.indexOf('_');
            result = keyStr.substring(0, endPosition);
        }

        // other type of auto-generated predicates
        // TBD

        // manual predicate
        // TBD

//        // Duong debug
//        System.out.println("getRelatedPredicateNameFromKey: keyStr = " + keyStr + " relatedPredName = " + result);

        return result;

    }

    /**
     * enhanced version of generatePredicateFromName
     * @param predName
     * @return
     */
    public static Predicate generatePredicateFromNameEnhanced(String predName, String monitorIpAddr, String monitorPortNumber){
        int predicateSubClass = inferPredicateSubClassFromName(predName);

        switch(predicateSubClass){
            case PREDICATE_SUBCLASS_GRAPH_EDGE:
                return new GraphEdgePredicateEnhanced(
                        predName,
                        monitorIpAddr,
                        monitorPortNumber);

//            case PREDICATE_SUBCLASS_CONJUNCTIVE:
//                  TBD
//                return new ConjunctivePredicate(
//                        0, predName,
//                        MultiplePredicateMonitor.getMonitorIp(),
//                        String.valueOf(MultiplePredicateMonitor.getMonitorPortNumber()),
//                        PredicateType.LINEAR,
//                        null);


            case PREDICATE_SUBCLASS_UNKNOWN:
            default:
                System.out.println("generatePredicateFromName: unknown predicate subclass for predicate name: " + predName);
                return null;
        }

    }

    /**
     * automatically infer the predicate type from predicate name, and generate an instance of predicate accordingly
     * @param predName
     * @return
     */
    public static Predicate generatePredicateFromName(String predName, String monitorIpAddr, String monitorPortNumber){
        int predicateSubClass = inferPredicateSubClassFromName(predName);

        switch(predicateSubClass){
            case PREDICATE_SUBCLASS_GRAPH_EDGE:
                return new GraphEdgePredicate(
                        predName,
                        monitorIpAddr,
                        monitorPortNumber);

//            case PREDICATE_SUBCLASS_CONJUNCTIVE:
//                  TBD
//                return new ConjunctivePredicate(
//                        0, predName,
//                        MultiplePredicateMonitor.getMonitorIp(),
//                        String.valueOf(MultiplePredicateMonitor.getMonitorPortNumber()),
//                        PredicateType.LINEAR,
//                        null);


            case PREDICATE_SUBCLASS_UNKNOWN:
            default:
                System.out.println("generatePredicateFromName: unknown predicate subclass for predicate name: " + predName);
                return null;
        }

    }

    //    public static Predicate generatePredicateFromName(String predName){
//        return generatePredicateFromName(predName, null, null);
//    }

    // return true if predName describe GraphEdgePredicate
    // e.g. 12_34
    static boolean doesPredicateNameDescribeGraphEdgePredicate(String predName){
        if(predName == null)
            return false;

        String[] nodeIds = predName.split("_");
        if(nodeIds.length != 2)
            return false;

        try{
            Integer.parseInt(nodeIds[0]);
            Integer.parseInt(nodeIds[1]);
        }catch(NumberFormatException nfe){
            return false;
        }

        return true;
    }

    static int inferPredicateSubClassFromName(String predName){
        if(doesPredicateNameDescribeGraphEdgePredicate(predName))
            return PREDICATE_SUBCLASS_GRAPH_EDGE;

        if(predName.contains("conjpred"))
            return PREDICATE_SUBCLASS_CONJUNCTIVE;

        return PREDICATE_SUBCLASS_UNKNOWN;

    }

    /**
     * This function generate a mapping reflecting a local snapshot content in which
     * the predicate is considered inactive.
     * By default, all variable is assigned value 0
     * Subclass can override it
     * @return
     */
    public Table<String, MonitorVectorClock, String> generateInactiveMapping(){
        Table<String, MonitorVectorClock, String> result = HashBasedTable.create();

        for(Map.Entry<String, String> entry : variableList.entrySet()){
            result.put(entry.getKey(), new MonitorVectorClock(), "0");
        }

        return result;
    }

    /**
     * Check if a predicate is active w.r.t. the current data at the server.
     * The concept of active predicate varies between predicate subclasses
     * Thus this method should be overriden by subclasses.
     * @param dataSet
     * @return
     */
    abstract public boolean isPredicateActiveAtServer(Table<String, MonitorVectorClock, String> dataSet);
}
