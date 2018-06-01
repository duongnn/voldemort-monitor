package predicatedetectionlib.common;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import predicatedetectionlib.common.clientgraph.ClientGraphNode;
import predicatedetectionlib.common.clientgraph.ClientGraphNodeType;
import predicatedetectionlib.common.predicate.*;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * Created by duongnn on 11/7/17.
 */
public class XmlUtils {
    // get the list of predicates user provided in an xml file
    public static Vector<Element> getElementListFromFile(String filename, String elementTag){
        Vector<Element> elementList = null;

        try {

            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new File(filename));

            doc.getDocumentElement().normalize();

            Element rootElement = doc.getDocumentElement();

            NodeList nodeList = rootElement.getElementsByTagName(elementTag);

            if(nodeList.getLength() == 0)
                return null;

            elementList = new Vector<>(nodeList.getLength());

            for (int i = 0; i < nodeList.getLength(); i++) {
                elementList.addElement((Element) nodeList.item(i));
            }
        } catch (Exception e) {
            System.out.println(" XmlUtils.getElementListFromFile ERROR" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        return elementList;
    }

    public static Vector<Element> getElementListFromNode(Element parentNode, String elementTag){
        NodeList nodeList = parentNode.getElementsByTagName(elementTag);

        if(nodeList.getLength() == 0)
            return null;

        Vector<Element> elementList = new Vector<>(nodeList.getLength());

        for (int i = 0; i < nodeList.getLength(); i++) {
            elementList.addElement((Element) nodeList.item(i));
        }

        return elementList;
    }

    // convert an xml element into a predicate
    public static Predicate elementToPredicate(Element predElement){
        // id
        int predicateId = Integer.valueOf(predElement.getElementsByTagName("id").item(0).getTextContent());

        // name
        String predicateName = predElement.getElementsByTagName("name").item(0).getTextContent();

        // subclass
        String predicateSubClass = predElement.getElementsByTagName("subclass").item(0).getTextContent();

        // monitor
        Element monitorAddr = (Element) predElement.getElementsByTagName("monitorAddr").item(0);
        String monitorIpAddr = monitorAddr.getElementsByTagName("ip").item(0).getTextContent();
        String monitorPortNumber = monitorAddr.getElementsByTagName("port").item(0).getTextContent();

        // type
        PredicateType type = PredicateType.fromCode(predElement.getElementsByTagName("type").item(0).getTextContent());

        // conjunctive clause list
        Vector<ConjunctiveClause> conjunctiveClauseList = new Vector<>();
        Element conjunctiveClauseListElement = (Element) predElement.getElementsByTagName("clauseList").item(0);
        Vector<Element> conjClauseElementList = getElementListFromNode(conjunctiveClauseListElement, "conjClause");
        for(int c = 0; c < conjClauseElementList.size(); c++){
            conjunctiveClauseList.addElement(elementToConjunctiveClause(conjClauseElementList.elementAt(c)));
        }

        switch(predicateSubClass){
            case "GraphEdgePredicate":
                return new GraphEdgePredicate(predicateName, monitorIpAddr, monitorPortNumber);

            case "ConjunctivePredicate":
                return new ConjunctivePredicate(predicateId, predicateName, monitorIpAddr, monitorPortNumber, type, conjunctiveClauseList);

            // other subclasses: TBD

            default:
                return null;
        }

//        return new Predicate(predicateId, monitorIpAddr, monitorPortNumber, type, conjunctiveClauseList);
    }

    // convert a predicate to xml element
    public static Element predicateToElement(Document xmlDoc, Predicate pred){
        Element predicateElement = xmlDoc.createElement("predicate");

        // add id
        Element idElement = xmlDoc.createElement("id");
        idElement.appendChild(xmlDoc.createTextNode(String.valueOf(pred.getPredicateId())));
        predicateElement.appendChild(idElement);

        // add monitor address
        Element monitorAddrElement = xmlDoc.createElement("monitorAddr");
        Element monitorIpElement = xmlDoc.createElement("ip");
        monitorIpElement.appendChild(xmlDoc.createTextNode(pred.getMonitorIpAddr()));
        Element monitorPortNumElement = xmlDoc.createElement("port");
        monitorPortNumElement.appendChild(xmlDoc.createTextNode(pred.getMonitorPortNumber()));
        monitorAddrElement.appendChild(monitorIpElement);
        monitorAddrElement.appendChild(monitorPortNumElement);
        predicateElement.appendChild(monitorAddrElement);

        // adding predicate type
        Element typeElement = xmlDoc.createElement("type");
        typeElement.appendChild(xmlDoc.createTextNode(pred.getType().getCode()));
        predicateElement.appendChild(typeElement);

        // adding conjunctive clause list
        Element conjClauseListElement = xmlDoc.createElement("clauseList");
        for(ConjunctiveClause clause : pred.getConjunctiveClauseList()){
            conjClauseListElement.appendChild(conjunctiveClauseToElement(xmlDoc, clause));
        }
        predicateElement.appendChild(conjClauseListElement);

        return predicateElement;

    }

    // convert an Xml Element to an instance of ConjunctiveClause
    public static ConjunctiveClause elementToConjunctiveClause(Element conjClauseElement){
        // id
        int clauseId = Integer.valueOf(conjClauseElement.getElementsByTagName("id").item(0).getTextContent());

        // variable list
        Vector<Element> varList = getElementListFromNode(conjClauseElement, "var");
        HashMap<String, String> variableList = new HashMap<>();
        for(int v = 0; v < varList.size(); v++){
            String varName = varList.elementAt(v).getElementsByTagName("name").item(0).getTextContent();
            String varValue = varList.elementAt(v).getElementsByTagName("value").item(0).getTextContent();
            variableList.put(varName, varValue);
        }

        return new ConjunctiveClause(clauseId, variableList);
    }

    // convert a ConjunctiveClause to an Xml Element
    public static Element conjunctiveClauseToElement(Document xmlDoc, ConjunctiveClause clause){
        Element conjClauseElement = xmlDoc.createElement("conjClause");

        // add id
        Element idElement = xmlDoc.createElement("id");
        idElement.appendChild(xmlDoc.createTextNode(String.valueOf(clause.getClauseId())));
        conjClauseElement.appendChild(idElement);

        // adding variable list
        for (Map.Entry<String, String> entry : clause.getVariableList().entrySet()) {
            Element varElement = xmlDoc.createElement("var");

            // variable name
            Element varNameElement = xmlDoc.createElement("name");
            varNameElement.appendChild(xmlDoc.createTextNode(entry.getKey()));
            varElement.appendChild(varNameElement);

            // variable value
            Element varValueElement = xmlDoc.createElement("value");
            varValueElement.appendChild(xmlDoc.createTextNode(entry.getValue()));
            varElement.appendChild(varValueElement);

            conjClauseElement.appendChild(varElement);
        }

        return conjClauseElement;
    }

    // convert an Xml Element to ClientGraphNode
    public static ClientGraphNode elementToClientGraphNode(Element cgnElement){
        // name
        String name = cgnElement.getElementsByTagName("name").item(0).getTextContent();

        // type
        ClientGraphNodeType type = ClientGraphNodeType.fromCode(
                                        cgnElement.getElementsByTagName("type").item(0).getTextContent());

        return new ClientGraphNode(name, type);
    }

}
