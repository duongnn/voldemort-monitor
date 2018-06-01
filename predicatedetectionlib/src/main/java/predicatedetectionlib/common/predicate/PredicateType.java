package predicatedetectionlib.common.predicate;

/**
 * Created by duongnn on 11/8/17.
 */
public enum PredicateType {
    LINEAR("linear", "linear-predicate"),
    SEMILINEAR("semilinear", "semi-linear-predicate");

    private final String code;
    private final String description;

    private PredicateType(String code, String description){
        this.code = code;
        this.description = description;
    }

    public String getCode(){
        return code;
    }
    public String getDescription(){
        return description;
    }

    public static PredicateType fromCode(String code){
        for(PredicateType pt : PredicateType.values()){
            if (pt.getCode().equals(code))
                return pt;
        }

        return null;
    }
}
