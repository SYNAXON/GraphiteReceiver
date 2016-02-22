package de.synaxon.graphitereceiver.domain;

import java.util.List;

public class Rule {

    private String target;
    private List<Replace> replaces;
    private List<Rename> renames;
    private List<String> match;
    private boolean stop;
    private boolean isLowerCase;
    private boolean isUpperCase;

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public List<Replace> getReplaces() {
        return replaces;
    }

    public void setReplaces(List<Replace> replaces) {
        this.replaces = replaces;
    }

    public List<Rename> getRenames() {
        return renames;
    }

    public void setRenames(List<Rename> renames) {
        this.renames = renames;
    }

    public List<String> getMatch() {
        return match;
    }

    public void setMatch(List<String> match) {
        this.match = match;
    }

    public boolean isStop() {
        return stop;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public boolean isLowerCase() {
        return isLowerCase;
    }

    public void setLowerCase(boolean isLowerCase) {
        this.isLowerCase = isLowerCase;
    }

    public boolean isUpperCase() {
        return isUpperCase;
    }

    public void setUpperCase(boolean isUpperCase) {
        this.isUpperCase = isUpperCase;
    }
}
