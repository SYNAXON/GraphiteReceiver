package de.synaxon.graphitereceiver.utils;

import com.vmware.ee.common.VimConnection;
import com.vmware.ee.statsfeeder.ExecutionContext;
import com.vmware.vim25.DynamicProperty;
import com.vmware.vim25.InvalidPropertyFaultMsg;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.ObjectContent;
import com.vmware.vim25.ObjectSpec;
import com.vmware.vim25.PropertyFilterSpec;
import com.vmware.vim25.PropertySpec;
import com.vmware.vim25.RetrieveOptions;
import com.vmware.vim25.RetrieveResult;
import com.vmware.vim25.RuntimeFaultFaultMsg;
import com.vmware.vim25.TraversalSpec;
import de.synaxon.graphitereceiver.domain.MapPrefixSuffix;
import de.synaxon.graphitereceiver.domain.Rule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Utils {

    private static Log logger = LogFactory.getLog(Utils.class);

    public static String[] splitCounterName(String counterName) {
        //should split string in a 3 componet array
        // [0] = groupName
        // [1] = metricName
        // [2] = rollup
        String[] result=new String[3];
        String[] tmp=counterName.split("[.]");
        //group Name
        result[0]=tmp[0];
        //rollup
        result[2]=tmp[tmp.length-1];
        result[1]=tmp[1];
        if ( tmp.length > 3){
            for(int i=2;i<tmp.length-1;++i) {
                result[1]=result[1]+"."+tmp[i];
            }
        }
        return result;
    }

    /**
     * initClusterHostMap is a self recursive method for generating VM/ESX to Cluster Hash Map.
     * In the first iteration it gathers all clusters and in consecutive calls for each cluster it updates Hash Map.
     * The logic here is use ComputeResource Entity as a base for gathering all virtual machines and ESX Hosts.
     * As part of configurations, GraphiteReceiver invokes this method at regular intervals (configured) and during runtime
     * if VM/ESX does not exist in the hash map.
     */
    public static boolean initClusterHostMap(String clusterName, ManagedObjectReference rootFolder, ExecutionContext context, Map<String,String> clusterMap){
        try {
            if(clusterName == null){
                clusterMap.clear();
            }
            VimConnection connection = context.getConnection();
            RetrieveResult retrieveResult = getRetrieveResult(clusterName, connection, rootFolder);

            while((retrieveResult != null) && (retrieveResult.getObjects() != null) && (retrieveResult.getObjects().size() > 0)){

                String token = retrieveResult.getToken();

                for(ObjectContent objectContent : retrieveResult.getObjects()){
                    List<DynamicProperty> dynamicProperties = objectContent.getPropSet();
                    if(clusterName != null){
                        String dpsGet = String.valueOf(dynamicProperties.get(0).getVal());
                        clusterMap.put(dpsGet.replace(" ", "_"), clusterName.replace(" ", "_"));
                    } else {
                        initClusterHostMap((String) (dynamicProperties.get(0).getVal()), objectContent.getObj(), context, clusterMap);
                    }
                }

                if (token == null) {
                    return true;
                }
                retrieveResult = connection.getVimPort().continueRetrievePropertiesEx(connection.getPropertyCollector(), token);
            }
            return true;
        } catch(Exception e){
            logger.fatal("Critical Error Detected.");
            logger.fatal(e.getLocalizedMessage());
            return false;
        }
    }

    private static TraversalSpec getTraversalSpec(String clusterName){
        TraversalSpec traversalSpec = new TraversalSpec();
        traversalSpec.setName("traverseEntities");
        traversalSpec.setPath("view");
        traversalSpec.setSkip(false);
        traversalSpec.setType("ContainerView");

        TraversalSpec traversalSpecAux = new TraversalSpec();
        if(clusterName == null){
            traversalSpecAux.setType("ComputeResource");
            traversalSpecAux.setPath("host");
        }else{
            traversalSpecAux.setType("HostSystem");
            traversalSpecAux.setPath("vm");
        }
        traversalSpecAux.setSkip(false);
        traversalSpec.getSelectSet().add(traversalSpecAux);
        return traversalSpec;
    }
    private static PropertySpec getPropertySpec(String type){
        PropertySpec propertySpec = new PropertySpec();
        propertySpec.setType(type);
        propertySpec.getPathSet().add("name");
        return propertySpec;
    }

    private static RetrieveResult getRetrieveResult(String clusterName, VimConnection connection, ManagedObjectReference rootFolder) throws RuntimeFaultFaultMsg, InvalidPropertyFaultMsg {
        List<String> clusterList = new ArrayList<String>();
        clusterList.add("ComputeResource");
        clusterList.add("HostSystem");
        clusterList.add("VirtualMachine");

        ManagedObjectReference rootFolderAux = (clusterName == null)? connection.getRootFolder():rootFolder;
        ManagedObjectReference viewManager = connection.getVimPort().createContainerView(connection.getViewManager(), rootFolderAux, clusterList, true);

        if(viewManager == null) {
            logger.debug("cViewRef is null: " + clusterName);
            return null;
        }
        logger.debug("cViewRef is not null: " + clusterName);

        ObjectSpec objectSpec = new ObjectSpec();
        objectSpec.setObj(viewManager);
        objectSpec.setSkip(true);
        objectSpec.getSelectSet().add(getTraversalSpec(clusterName));

        PropertyFilterSpec propertyFilterSpec = new PropertyFilterSpec();
        propertyFilterSpec.getObjectSet().add(objectSpec);

        if(clusterName == null){
            propertyFilterSpec.getPropSet().add(getPropertySpec("ComputeResource"));
        }else{
            propertyFilterSpec.getPropSet().add(getPropertySpec("HostSystem"));
            propertyFilterSpec.getPropSet().add(getPropertySpec("VirtualMachine"));
        }
        List<PropertyFilterSpec> propertyFilterSpecs = new LinkedList<PropertyFilterSpec>();
        propertyFilterSpecs.add(propertyFilterSpec);
        return connection.getVimPort().retrievePropertiesEx(connection.getPropertyCollector(), propertyFilterSpecs, new RetrieveOptions());
    }



    public static String getNode(Map<String,String> graphiteTree, Boolean place_rollup_in_the_end, Boolean isHostMap, Map<String, MapPrefixSuffix> hostMap) {

        String graphite_prefix = graphiteTree.get("graphite_prefix");
        String cluster = graphiteTree.get("cluster");
        String eName = graphiteTree.get("eName");
        String groupName = graphiteTree.get("groupName");
        String instanceName = graphiteTree.get("instanceName");
        String metricName = graphiteTree.get("metricName");
        String statType = graphiteTree.get("statType");
        String rollup = graphiteTree.get("rollup");
        String counterName = graphiteTree.get("counterName");
        String hostName = graphiteTree.get("hostName");

        StringBuilder nodeBuilder = new StringBuilder();

        if ("null".equals(cluster)) {
            logger.warn("The cluster is null (String)");
        }
        if(isHostMap) {
            if ((hostMap.size() > 0) && (hostName != null && !hostName.equals(""))) {
                MapPrefixSuffix mapPrefixSuffix = hostMap.get(hostName.toLowerCase());
                String filePrefix;
                String fileSufix;
                if (mapPrefixSuffix != null) {
                    filePrefix = mapPrefixSuffix.getPrefix();
                    fileSufix = mapPrefixSuffix.getSufix();
                } else {
                    return null;
                }
                if (filePrefix != null && fileSufix != null) {
                    nodeBuilder.append(filePrefix).append(".");
                    nodeBuilder.append(hostName).append(".");
                    nodeBuilder.append(fileSufix).append(".");
                } else {
                    nodeBuilder.append(graphite_prefix).append(".");
                    nodeBuilder.append((cluster == null || ("".equals(cluster))) ? "" : cluster + ".");
                    nodeBuilder.append(eName).append(".");
                }
            } else {
                if("vm".equals(eName) || (eName != null && eName.contains("vm"))){
                    return null;
                } else {
                    nodeBuilder.append(graphite_prefix).append(".");
                    nodeBuilder.append((cluster == null || ("".equals(cluster) || ("null".equals(cluster)))) ? "" : cluster + ".");
                    nodeBuilder.append(eName).append(".");
                }
            }
        } else {
            nodeBuilder.append(graphite_prefix).append(".");
            nodeBuilder.append((cluster == null || ("".equals(cluster) || ("null".equals(cluster)))) ? "" : cluster + ".");
            nodeBuilder.append(eName).append(".");
        }
        nodeBuilder.append(groupName).append(".");
        nodeBuilder.append((instanceName == null || ("".equals(instanceName))) ? "" : instanceName + ".");
        nodeBuilder.append(metricName).append("_");
        if(place_rollup_in_the_end){
            nodeBuilder.append(statType).append("_");
            nodeBuilder.append(rollup);
        } else {
            nodeBuilder.append(rollup).append("_");
            nodeBuilder.append(statType);
        }
        logger.debug((instanceName == null || ("".equals(instanceName))) ?
                        "GP :" + graphite_prefix + " EN: " + eName + " CN: " + counterName + " ST: " + statType :
                        "GP :" + graphite_prefix + " EN: " + eName + " GN :" + groupName + " IN :" + instanceName + " MN :" + metricName + " ST: " + statType + " RU: " + rollup
        );

        if (cluster == null || "".equals(cluster) || "null".equals(cluster)) {
            logger.debug("The cluster is null - " + eName);
            return null;
        }
        return nodeBuilder.toString();
    }

    public static String getEName(boolean entityPrefix, boolean useFqdn, String entityName, String parseEntityName, List<Rule> rules ){
        String prefix = "";
        String sufix = "";
        if(entityPrefix) {
            if(entityName.contains("[VirtualMachine]")) {
                prefix = "vm.";
            }else if (entityName.contains("[HostSystem]")) {
                prefix = "esx.";
            }else if (entityName.contains("[Datastore]")) {
                prefix="dts.";
            }else if (entityName.contains("[ResourcePool]")) {
                prefix="rp.";
            }
        }
        if(!useFqdn && entityName.contains("[HostSystem]")){
            sufix = parseEntityName.split("[.]", 2)[0];
        } else {
            sufix = parseEntityName.replace('.', '_').replace(' ', '_').replace('-', '_');
        }

        if(rules != null && rules.size() > 0){
            sufix = RuleUtils.applyRules(sufix, rules);
        }

        return prefix + sufix;
    }
    public static boolean isUpper(String s) {
        for(char c : s.toCharArray()) {
            if(! Character.isUpperCase(c))
                return false;
        }
        return true;
    }

    public static int calculateIteration(long frequencyInSeconds, String refreshInterval, String type){

        try {
            long cacheRefreshInterval = Long.valueOf(refreshInterval);
            if (cacheRefreshInterval < frequencyInSeconds) {
                logger.debug(type + " attribute is not set or not supported.");
                logger.debug(type + " set: " + frequencyInSeconds + " seconds");
                return 1;
            } else {
                if(cacheRefreshInterval%frequencyInSeconds == 0) {
                    return (int) (cacheRefreshInterval/frequencyInSeconds);
                } else {
                    return (int) (cacheRefreshInterval/frequencyInSeconds);
                }
            }
        } catch (NumberFormatException e) {
            logger.debug(type + " attribute is not set or not supported.");
            logger.debug(type + " set: " + frequencyInSeconds + " seconds");
            return 1;
        }
    }

}

