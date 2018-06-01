package predicatedetectionlib.common.clientgraph;

import java.util.HashMap;

/**
 * Created by duongnn on 11/21/17.
 * The class describing a node in the line graph
 * This class will be used by clientGraph program
 */
public class ClientGraphNode {
    String name;
    ClientGraphNodeType type;

    // A mapping of a neighbor (name) and the lock associated with the link
    // between this node and that neighbor
    private HashMap<String, ClientGraphPetersonLock> lockHashMap;

    public ClientGraphNode(String name, ClientGraphNodeType type){
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ClientGraphNodeType getType() {
        return type;
    }

    public void setType(ClientGraphNodeType type) {
        this.type = type;
    }

    public HashMap<String, ClientGraphPetersonLock> getLockHashMap() {
        return lockHashMap;
    }

    public void setLockHashMap(HashMap<String, ClientGraphPetersonLock> lockHashMap) {
        this.lockHashMap = lockHashMap;
    }
}
