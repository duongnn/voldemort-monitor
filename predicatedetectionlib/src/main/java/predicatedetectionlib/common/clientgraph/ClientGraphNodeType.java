package predicatedetectionlib.common.clientgraph;

/**
 * Created by duongnn on 11/21/17.
 */

public enum ClientGraphNodeType {
    INTERNAL("internal", "internal node of graph"),
    BORDER("border", "bordering node of graph, i.e. it is connected to node responsible by another client");

    private final String code;
    private final String description;

    private ClientGraphNodeType(String code, String description){
        this.code = code;
        this.description = description;
    }

    public String getCode(){
        return code;
    }
    public String getDescription(){
        return description;
    }

    public static ClientGraphNodeType fromCode(String code){
        for(ClientGraphNodeType nt : ClientGraphNodeType.values()){
            if (nt.getCode().equals(code))
                return nt;
        }

        return null;
    }

}
