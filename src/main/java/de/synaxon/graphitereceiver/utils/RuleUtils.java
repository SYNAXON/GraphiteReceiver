package de.synaxon.graphitereceiver.utils;

import de.synaxon.graphitereceiver.domain.Rename;
import de.synaxon.graphitereceiver.domain.Replace;
import de.synaxon.graphitereceiver.domain.Rule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class RuleUtils {

    private static Log logger = LogFactory.getLog(RuleUtils.class);

    public static String applyRules(String instanceName, List<Rule> rules){
        for(Rule rule:rules){
            instanceName = applyRules(instanceName, rule);
        }
        return instanceName;
    }

    private static String applyRules(String value, Rule rule){
        if(rule.isUpperCase()){
            value = value.toUpperCase();
        }
        if(rule.isLowerCase()) {
            value = value.toLowerCase();
        }
        if(rule.getReplaces() != null && rule.getReplaces().size() > 0) {
            for(Replace replace: rule.getReplaces()){
                value = value.replace(replace.getSearchValue(), replace.getNewValue());
            }
        }
        if(rule.getRenames() != null && rule.getRenames().size() > 0) {
            for(Rename rename: rule.getRenames()){
                if (value.equalsIgnoreCase(rename.getOldName())) {
                    boolean isUppercase = Utils.isUpper(value);
                    value = rename.getNewName();
                    if(isUppercase) {
                        value = value.toUpperCase();
                    }
                }
            }
        }
        return value;
    }





}
