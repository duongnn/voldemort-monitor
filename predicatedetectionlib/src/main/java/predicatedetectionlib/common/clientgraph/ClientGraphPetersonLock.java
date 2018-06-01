package predicatedetectionlib.common.clientgraph;

/**
 * Created by duongnn on 11/21/17.
 *
 * Represent a lock between a client node and one of its neighbor
 */
public class ClientGraphPetersonLock {
    /*
        Example:
            nodeName = "15"
            neighborName = "34"
            nodeFlagVar = "flag15_34_15"
            neighborFlagVar = "flag15_34_34"
            turnVar = "turn15_34"

        flag15_34_15            flag15_34_34
        +-----+    turn15_34   +-----+
        | 15  |----------------| 34  |
        +-----+                +-----+
     */

    private String nodeName;            // name of client node
    private String neighborName;        // name of its neighbor
    private String nodeFlagVar;         // flag variable on client node side
    private String neighborFlagVar;     // flag variable on the neighbor side
    private String turnVar;             // turn variable


    /**
     * @param nodeName
     * @param neighborName
     * @param nodeFlagVar
     * @param neighborFlagVar
     * @param turnVar
     */
    public ClientGraphPetersonLock(String nodeName, String neighborName, String nodeFlagVar, String neighborFlagVar, String turnVar){
        this.nodeName = nodeName;
        this.neighborName = neighborName;
        this.nodeFlagVar = nodeFlagVar;
        this.neighborFlagVar = neighborFlagVar;
        this.turnVar = turnVar;
    }

    public String getNeighborName() {
        return neighborName;
    }

    public void setNeighborName(String neighborName) {
        this.neighborName = neighborName;
    }

    public String getNodeFlagVar() {
        return nodeFlagVar;
    }

    public void setNodeFlagVar(String nodeFlagVar) {
        this.nodeFlagVar = nodeFlagVar;
    }

    public String getTurnVar() {
        return turnVar;
    }

    public void setTurnVar(String turnVar) {
        this.turnVar = turnVar;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNeighborFlagVar() {
        return neighborFlagVar;
    }

    public void setNeighborFlagVar(String neighborFlagVar) {
        this.neighborFlagVar = neighborFlagVar;
    }

    // return the string containing both id in increasing order, e.g. 15_34, not 34_15
    public String getBothIdString(){
        return turnVar.substring(4);
    }

    // return string like 15_34_15 if node is 15
    //                    15_34_34 if node is 34
    public String getBothIdAndIdString(){
        return nodeFlagVar.substring(4);
    }
}
