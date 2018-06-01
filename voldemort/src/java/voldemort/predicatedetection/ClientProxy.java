package voldemort.predicatedetection;

import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;

/**
 * Created by duongnn on 12/22/17.
 * This class is for supporting proxy feature
 */
public class ClientProxy {
    // Duong: variable for proxy status
    private static boolean proxyIsEnabled;
    private static String proxyIpAddress;
    private static String proxyPortNumber;
    private static String proxyBasePortNumber; // all clients have the same proxyBasePortNumber

    public static boolean isProxyIsEnabled() {
        return proxyIsEnabled;
    }

    public static void setProxyIsEnabled(boolean proxyIsEnabled) {
        ClientProxy.proxyIsEnabled = proxyIsEnabled;
    }

    public static String getProxyIpAddress() {
        return proxyIpAddress;
    }

    public static void setProxyIpAddress(String proxyIpAddress) {
        ClientProxy.proxyIpAddress = proxyIpAddress;
    }

    public static String getProxyPortNumber() {
        return proxyPortNumber;
    }

    public static void setProxyPortNumber(String proxyPortNumber) {
        ClientProxy.proxyPortNumber = proxyPortNumber;
    }

    public static String getProxyBasePortNumber() {
        return proxyBasePortNumber;
    }

    public static void setProxyBasePortNumber(String proxyBasePortNumber) {
        ClientProxy.proxyBasePortNumber = proxyBasePortNumber;
    }

    public static String replaceServersByProxies(String originalMetadata){
        String newMetadataString;

        // Duong debug
        System.out.println(" proxyIsEnabled = " + proxyIsEnabled);
        System.out.println(" proxyIpAddress = " + proxyIpAddress);
        System.out.println(" proxyPortNumber = " + proxyPortNumber);
        System.out.println(" proxyBasePortNumber = " + proxyBasePortNumber);
        System.out.println();

        int baseProxyPortNumber = Integer.parseInt(proxyBasePortNumber);

        try {
            // read xml string into document and parse it
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(originalMetadata.getBytes()));

            // read all servers
            org.w3c.dom.NodeList serverList = doc.getElementsByTagName("server");
            int numberOfServers = serverList.getLength();
            for(int i = 0; i < numberOfServers; i++){
                org.w3c.dom.Node currentServer = serverList.item(i);
                org.w3c.dom.NodeList currentServerElements = currentServer.getChildNodes();

                // replace the element "host" and "socket-port"
                for(int j = 0; j < currentServerElements.getLength(); j++){
                    org.w3c.dom.Node serverElement = currentServerElements.item(j);

                    if("host".equals(serverElement.getNodeName())){
                        serverElement.setTextContent(proxyIpAddress);
                    }
                    if("socket-port".equals(serverElement.getNodeName())){
                        serverElement.setTextContent(String.valueOf(baseProxyPortNumber++));
                    }
                }
            }

            // write the modified document into new string
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            newMetadataString = writer.getBuffer().toString();

        }catch(Exception e){
            System.out.println(" Error: " + e.getMessage());
            e.printStackTrace();

            return originalMetadata;
        }

        return newMetadataString;
    }

}
