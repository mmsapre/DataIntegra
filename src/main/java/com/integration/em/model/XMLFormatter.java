package com.integration.em.model;

import com.integration.em.processing.Processable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
public abstract class XMLFormatter<RecordType> {

    protected Element createTextElement(String name, String value, Document doc) {
        Element elem = doc.createElement(name);
        if (value != null) {
            elem.appendChild(doc.createTextNode(value));
        }
        return elem;
    }


    public void writeXML(File outputFile, Processable<RecordType> dataset)
            throws ParserConfigurationException, TransformerException,
            FileNotFoundException {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;

        builder = factory.newDocumentBuilder();
        Document doc = builder.newDocument();
        Element root = createRootElement(doc);

        doc.appendChild(root);

        for (RecordType record : dataset.get()) {
            root.appendChild(createElementFromRecord(record, doc));
        }

        TransformerFactory tFactory = TransformerFactory.newInstance();
        Transformer transformer;
        transformer = tFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(
                "{http://xml.apache.org/xslt}indent-amount", "2");
        DOMSource source = new DOMSource(root);
        StreamResult result = new StreamResult(new FileOutputStream(outputFile));

        transformer.transform(source, result);

    }

    public abstract Element createRootElement(Document doc);

    public abstract Element createElementFromRecord(RecordType record, Document doc);
}
