package de.synaxon.graphitereceiver.core.xml;

import de.synaxon.graphitereceiver.domain.Rename;
import de.synaxon.graphitereceiver.domain.Replace;
import de.synaxon.graphitereceiver.domain.Rule;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ReadRules {

    private Document dom;
    private Map<String, List<Rule>> rules;

    public ReadRules(String path){
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            this.dom = db.parse(path);
            this.readXml();
        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (SAXException se) {
            se.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void readXml(){
        Element root = this.dom.getDocumentElement();
        NodeList nodeList = root.getChildNodes();
        this.rules = new HashMap<String, List<Rule>>();
        if (nodeList != null && nodeList.getLength() > 0) {
            Rule rule;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if ("Rule".equals(node.getNodeName())) {
                    rule = new Rule();
                    Element element = (Element) node;
                    List<Replace> replaces = new LinkedList<Replace>();
                    List<Rename> renames = new LinkedList<Rename>();
                    List<String> match = new LinkedList<String>();
                    boolean isLowerCase = false;
                    boolean isUpperCase = false;
                    boolean stop = false;
                    NodeList nodeRuleList = node.getChildNodes();
                    for (int y = 0; y < nodeRuleList.getLength(); y++) {
                        Node nodeRule = nodeRuleList.item(y);
                        if("replace".equals(nodeRule.getNodeName())){
                            Element elementRule = (Element) nodeRule;
                            Replace replace;
                            replace = new Replace();
                            replace.setSearchValue(getStringAttribute(elementRule, "searchValue"));
                            replace.setNewValue(getStringAttribute(elementRule, "newValue"));
                            replaces.add(replace);
                        } else if("Rename".equals(nodeRule.getNodeName())){
                            Element elementRule = (Element) nodeRule;
                            Rename rename;
                            rename = new Rename();
                            rename.setOldName(getStringAttribute(elementRule, "oldName"));
                            rename.setNewName(getStringAttribute(elementRule, "newName"));
                            renames.add(rename);
                        } else if("Match".equals(nodeRule.getNodeName())){
                            Element elementRule = (Element) nodeRule;
                            match.add(getStringValue(elementRule, "Match"));
                        } else if("LowerCase".equals(nodeRule.getNodeName())){
                            Element elementRule = (Element) nodeRule;
                            isLowerCase = getBooleanValue(element, "LowerCase");
                        } else if("UpperCase".equals(nodeRule.getNodeName())){
                            Element elementRule = (Element) nodeRule;
                            isUpperCase = getBooleanValue(element, "UpperCase");
                        } else if("Stop".equals(nodeRule.getNodeName())){
                            Element elementRule = (Element) nodeRule;
                            stop = getBooleanValue(element, "Stop");
                        }
                    }
                    rule.setTarget(getStringAttribute(element, "target"));
                    rule.setLowerCase(isLowerCase);
                    rule.setUpperCase(isUpperCase);
                    rule.setMatch(match);
                    rule.setRenames(renames);
                    rule.setReplaces(replaces);
                    rule.setStop(stop);

                    if(this.rules.get(getStringAttribute(element, "target")) != null){
                        List<Rule> ruleAux = this.rules.get(getStringAttribute(element, "target"));
                        ruleAux.add(rule);
                        this.rules.put(getStringAttribute(element, "target"), ruleAux);
                    } else {
                        List<Rule> ruleAux = new LinkedList<Rule>();
                        ruleAux.add(rule);
                        this.rules.put(getStringAttribute(element, "target"), ruleAux);
                    }
                }
            }
        }
    }

    private String getStringValue(Element element, String tagName) {
        String string = null;
        NodeList nl = element.getElementsByTagName(tagName);
        if (nl != null && nl.getLength() > 0) {
            Element aux = (Element) nl.item(0);
            string = aux.getFirstChild().getNodeValue();
        }
        return string;
    }

    private boolean getBooleanValue(Element element, String tagName) {
        String bool = "false";
        if(getStringValue(element, tagName) != null){
            bool = getStringValue(element, tagName);
        }
        return Boolean.valueOf(bool);
    }

    private String getStringAttribute(Element element, String tagName) {
        return element.getAttribute(tagName);
    }

    public Map<String, List<Rule>> getRules() {
        return rules;
    }

    public void setRules(Map<String, List<Rule>> rules) {
        this.rules = rules;
    }
}
