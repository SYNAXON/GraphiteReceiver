package de.synaxon.graphitereceiver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Alberto Pascual on 16/09/15.
 */
public class MapperPrefixSuffix {

    private File hostnameMap;
    private final String separator = ";";
    Log logger = LogFactory.getLog(MapperPrefixSuffix.class);

    public MapperPrefixSuffix(String path) throws FileNotFoundException {
        logger.warn("hostname map no exist or is empty");
        this.hostnameMap = new File(path);
        if(this.hostnameMap.exists()) {
            if(this.hostnameMap.length() == 0) {
                logger.warn("hostname map no exist or is empty");
            }
        } else {
            throw new FileNotFoundException();
        }

    }

    public Map<String, MapPrefixSuffix> getAllMapper(){
        Map<String, MapPrefixSuffix> hostMap = new HashMap<String, MapPrefixSuffix>();
        BufferedReader bufferedReader = bufferedReaderFactory();
        String line;
        try {
            while((line = bufferedReader.readLine()) != null) {
                if(!line.startsWith("#")) {
                    String[] lineFound = line.split(this.separator);
                    MapPrefixSuffix mapPrefixSuffix = new MapPrefixSuffix(lineFound[1], lineFound[2]);
                    hostMap.put(lineFound[0], mapPrefixSuffix);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return hostMap;
    }
    public Map<String, MapPrefixSuffix> mapHostname(String hostname){
        BufferedReader bufferedReader = bufferedReaderFactory();
        String line;
        Boolean found = false;
        Map<String, MapPrefixSuffix> hostMap = new HashMap<String, MapPrefixSuffix>();
        try {

            while((line = bufferedReader.readLine()) != null && !found) {
                if(!line.startsWith("#") && line.contains(hostname) && line.contains(this.separator)) {
                    String[] lineFound = line.split(this.separator);
                    MapPrefixSuffix mapPrefixSuffix;
                    for(String split: Arrays.asList(lineFound)){
                        if(split.equals(hostname)){
                            mapPrefixSuffix = new MapPrefixSuffix(lineFound[1], lineFound[2]);
                            hostMap.put(lineFound[0], mapPrefixSuffix);
                            found = true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(found) {
            return hostMap;
        } else {
            return null;
        }
    }



    private BufferedReader bufferedReaderFactory(){
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;

        try {
            fileReader = new FileReader(this.hostnameMap);
            bufferedReader = new BufferedReader(fileReader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return bufferedReader;
    }

}
