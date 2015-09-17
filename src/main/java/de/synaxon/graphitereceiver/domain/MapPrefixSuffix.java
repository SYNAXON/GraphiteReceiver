package de.synaxon.graphitereceiver.domain;

public class MapPrefixSuffix {

    private String prefix;
    private String sufix;

    //public MapPrefixSuffix(){}

    public MapPrefixSuffix(String prefix, String sufix) {
        this.prefix = prefix;
        this.sufix = sufix;
    }

    public String getPrefix() {
        return prefix;
    }

//    public void setPrefix(String prefix) {
//        this.prefix = prefix;
//    }

    public String getSufix() {
        return sufix;
    }

//    public void setSufix(String suffix) {
//        this.sufix = suffix;
//    }
}
