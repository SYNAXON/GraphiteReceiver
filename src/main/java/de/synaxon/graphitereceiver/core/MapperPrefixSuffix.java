package de.synaxon.graphitereceiver.core;

import de.synaxon.graphitereceiver.domain.MapPrefixSuffix;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapperPrefixSuffix {

    private File hostnameMap;

    public MapperPrefixSuffix(String path) throws FileNotFoundException {
        Log logger = LogFactory.getLog(MapperPrefixSuffix.class);
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
        String separator = ";";
        Map<String, MapPrefixSuffix> hostMap = new HashMap<String, MapPrefixSuffix>();
        BufferedReader bufferedReader = bufferedReaderFactory();
        String line;
        try {
            while((line = bufferedReader.readLine()) != null) {
                if(!line.startsWith("#")) {
                    String[] lineFound = line.split(separator);
                    MapPrefixSuffix mapPrefixSuffix = new MapPrefixSuffix(lineFound[1], lineFound[2]);
                    hostMap.put(lineFound[0].toLowerCase(), mapPrefixSuffix);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return hostMap;
    }

    private BufferedReader bufferedReaderFactory(){
        FileReader fileReader;
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
