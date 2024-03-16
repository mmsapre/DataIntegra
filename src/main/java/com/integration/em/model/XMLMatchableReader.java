package com.integration.em.model;

import lombok.extern.slf4j.Slf4j;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
@Slf4j
public abstract class XMLMatchableReader<RecordType extends Matchable, SchemaElementType extends Matchable> {


    protected String getValueFromChildElement(Node node, String childName) {

        // get all child nodes
        NodeList children = node.getChildNodes();

        // iterate over the child nodes until the node with childName is found
        for (int j = 0; j < children.getLength(); j++) {
            Node child = children.item(j);

            // check the node type and the name
            if (child.getNodeType() == Node.ELEMENT_NODE
                    && child.getNodeName().equals(childName)) {

                return child.getTextContent().trim();

            }
        }

        return null;
    }

    protected List<String> getListFromChildElement(Node node, String childName) {

        // get all child nodes
        NodeList children = node.getChildNodes();

        // iterate over the child nodes until the node with childName is found
        for (int j = 0; j < children.getLength(); j++) {
            Node child = children.item(j);

            // check the node type and name
            if (child.getNodeType() == Node.ELEMENT_NODE
                    && child.getNodeName().equals(childName)) {

                // prepare a list to hold all values
                List<String> values = new ArrayList<>(child.getChildNodes()
                        .getLength());

                // iterate the value nodes
                for (int i = 0; i < child.getChildNodes().getLength(); i++) {
                    Node valueNode = child.getChildNodes().item(i);
                    String value = valueNode.getTextContent().trim();

                    // add the value
                    values.add(value);
                }

                return values;
            }
        }

        return null;
    }

    protected <ItemType extends Matchable> List<ItemType> getObjectListFromChildElement(
            Node node, String childName, String objectNodeName,
            XMLMatchableReader<ItemType, SchemaElementType> factory, String provenanceInfo) {

        // get all child nodes
        NodeList children = node.getChildNodes();

        // iterate over the child nodes until the node with childName is found
        for (int j = 0; j < children.getLength(); j++) {
            Node child = children.item(j);

            // check the node type and name
            if (child.getNodeType() == Node.ELEMENT_NODE
                    && child.getNodeName().equals(childName)) {

                // prepare a list to hold all values
                List<ItemType> values = new ArrayList<>(child.getChildNodes()
                        .getLength());

                // iterate the value nodes
                for (int i = 0; i < child.getChildNodes().getLength(); i++) {
                    Node valueNode = child.getChildNodes().item(i);

                    // check the node type and name
                    if (valueNode.getNodeType() == Node.ELEMENT_NODE
                            && valueNode.getNodeName().equals(objectNodeName)) {
                        // add the value
                        values.add(factory.createModelFromElement(valueNode,
                                provenanceInfo));
                    }
                }

                return values;
            }
        }

        return null;
    }

    protected void initialiseDataset(DataSet<RecordType, SchemaElementType> dataset) {

    }

    public void loadFromXML(File dataSource, String recordPath, DataSet<RecordType, SchemaElementType> dataset) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {

        // initialise the dataset
        initialiseDataset(dataset);

        // create objects for reading the XML file
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        builder = factory.newDocumentBuilder();
        Document doc = builder.parse(dataSource);

        // prepare the XPath that selects the entries
        XPathFactory xPathFactory = XPathFactory.newInstance();
        XPath xpath = xPathFactory.newXPath();
        XPathExpression expr = xpath.compile(recordPath);

        // execute the XPath to get all entries
        NodeList list = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

        if (list.getLength() == 0) {
            log.error("No elements matching the XPath ("
                    + recordPath + ") found in the input file "
                    + dataSource.getAbsolutePath());
        } else {
            log.info(String.format("Loading %d elements from %s",
                    list.getLength(), dataSource.getName()));

            // create entries from all nodes matching the XPath
            for (int i = 0; i < list.getLength(); i++) {

                // create the entry, use file name as provenance information
                RecordType record = createModelFromElement(
                        list.item(i), dataSource.getName());

                if (record != null) {
                    // add it to the data set
                    dataset.add(record);
                } else {
                    log.info(String.format(
                            "Could not generate entry for ", list.item(i)
                                    .getTextContent()));
                }
            }
        }
    }

    public abstract RecordType createModelFromElement(Node node, String provenanceInfo);



}
