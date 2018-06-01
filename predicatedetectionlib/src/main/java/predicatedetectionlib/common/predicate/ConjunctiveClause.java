package predicatedetectionlib.common.predicate;

import com.google.common.collect.Table;
import predicatedetectionlib.common.LocalSnapshotContent;
import predicatedetectionlib.versioning.MonitorVectorClock;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by duongnn on 11/20/17.
 *
 * A conjunctive clauses
 * So far, it is a mapping of variable names and variable values
 * The clause is true when all the mapping are satisfied
 */

public class ConjunctiveClause {
    private int clauseId;

    // mapping of variable names and variable values
    private HashMap<String, String> variableList;

    public int getClauseId() {
        return clauseId;
    }

    public void setClauseId(int clauseId) {
        this.clauseId = clauseId;
    }

    public HashMap<String, String> getVariableList() {
        return variableList;
    }

    public void setVariableList(HashMap<String, String> variableList) {
        this.variableList = variableList;
    }

    public ConjunctiveClause(int clauseId, HashMap<String, String> variableList){
        this.clauseId = clauseId;
        this.variableList = variableList;
    }

    // conjunctive clause is true when all variables have the specified value
    public boolean evaluateClause(LocalSnapshotContent localSnapshotContent){
        // check if local snapshot content is just a place holder
        if(localSnapshotContent == null)
            return false;

        Table<String, MonitorVectorClock, String> localState = localSnapshotContent.getLocalState();

        for(String varName : variableList.keySet()){
            String desiredValue = variableList.get(varName);

            Map<MonitorVectorClock, String> verValMap = localState.row(varName);

            // A variable could have multiple concurrent versions. If any of the version has desired value
            // then variable is considered to has the desired value
            // If no version has the desired value, then the variable does not have the desired value
            // and the conjunctive clause would be false
            if(!verValMap.containsValue(desiredValue))
                return false;
        }

        return true;
    }

    // string representation of a conjunctive clause
    public String toString(int indent){
        String indentStr = new String(new char[indent]).replace("\0", " ");

        StringBuilder result = new StringBuilder();

        result.append(indentStr + "conjClause:\n");
        result.append(indentStr + "  id: " + getClauseId() + "\n");
        variableList.forEach((k,v)->{
            result.append(indentStr + "    var:\n");
            result.append(indentStr + "      name: " + k + "\n");
            result.append(indentStr + "      value: " + v + "\n");
        });

        return result.toString();
    }

    public String toString(){
        return toString(0);
    }


}
