package de.synaxon.graphitereceiver.utils;

import com.vmware.ee.common.VimConnection;
import com.vmware.ee.statsfeeder.ExecutionContext;
import com.vmware.vim25.DynamicProperty;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.ObjectContent;
import com.vmware.vim25.ObjectSpec;
import com.vmware.vim25.PropertyFilterSpec;
import com.vmware.vim25.PropertySpec;
import com.vmware.vim25.RetrieveOptions;
import com.vmware.vim25.RetrieveResult;
import com.vmware.vim25.TraversalSpec;
import de.synaxon.graphitereceiver.domain.MapPrefixSuffix;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
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
     * getCluster returns associated cluster to the calling method. If requested VM/ESX managed entity does not exist in the cache,
     * it refreshes the cache.
     *
     * Potential Bottlenecks: If too many new VirtualMachines/ESX hosts added during runtime (between cache refresh intervals - this.cacheRefreshInterval)
     *                        may affect the performance because of too many vCenter connections and cache refreshments. We have tested & verified
     */
    public static String getCluster(String entity, ExecutionContext context, Map<String,String> clusterMap){
        try{
            String value = String.valueOf(clusterMap.get(entity));
            if(value == null){
                logger.warn("Cluster Not Found for Managed Entity " + entity);
                logger.warn("Reinitializing Cluster Entity Map");
                Utils.initClusterHostMap(null, null, context, clusterMap);
                value = String.valueOf(clusterMap.get(entity));
            }
            return value;
        }catch(Exception e){
            return null;
        }
    }

    /**
     * initClusterHostMap is a self recursive method for generating VM/ESX to Cluster Hash Map.
     * In the first iteration it gathers all clusters and in consecutive calls for each cluster it updates Hash Map.
     * The logic here is use ComputeResource Entity as a base for gathering all virtual machines and ESX Hosts.
     * As part of configurations, GraphiteReceiver invokes this method at regular intervals (configured) and during runtime
     * if VM/ESX does not exist in the hash map.
     */
    public static boolean initClusterHostMap(String ClusterName, ManagedObjectReference ClusterMor, ExecutionContext context, Map<String,String> clusterMap){        try {
            logger.debug("initClusterHostMap Begin");
            boolean retVal = true;

            VimConnection connection = context.getConnection();

            ManagedObjectReference viewMgrRef = connection.getViewManager();
            ManagedObjectReference propColl = connection.getPropertyCollector();
            List<String> clusterList = new ArrayList<String>();
            clusterList.add("ComputeResource");
            clusterList.add("HostSystem");
            clusterList.add("VirtualMachine");
            ManagedObjectReference rootFolder;
            if(ClusterName == null){
                rootFolder = connection.getRootFolder();
                clusterMap.clear();
            }else {
                rootFolder = ClusterMor;
            }
            ManagedObjectReference cViewRef = connection.getVimPort().createContainerView(viewMgrRef, rootFolder, clusterList, true);
            if(cViewRef != null){
                logger.debug("cViewRef is not null: " + ClusterName);
            } else {
                logger.debug("cViewRef is null: " + ClusterName);
                return false;
            }

            TraversalSpec tSpec = new TraversalSpec();
            tSpec.setName("traverseEntities");
            tSpec.setPath("view");
            tSpec.setSkip(false);
            tSpec.setType("ContainerView");

            ObjectSpec oSpec = new ObjectSpec();
            oSpec.setObj(cViewRef);
            oSpec.setSkip(true);
            oSpec.getSelectSet().add(tSpec);

            TraversalSpec tSpecC = new TraversalSpec();
            if(ClusterName == null){
                tSpecC.setType("ComputeResource");
                tSpecC.setPath("host");
                tSpecC.setSkip(false);
            }else{
                tSpecC.setType("HostSystem");
                tSpecC.setPath("vm");
                tSpecC.setSkip(false);
            }
            tSpec.getSelectSet().add(tSpecC);

            PropertyFilterSpec fSpec = new PropertyFilterSpec();
            fSpec.getObjectSet().add(oSpec);

            if(ClusterName == null){
                PropertySpec pSpecC = new PropertySpec();
                pSpecC.setType("ComputeResource");
                pSpecC.getPathSet().add("name");
                fSpec.getPropSet().add(pSpecC);
            }else{
                PropertySpec pSpecH = new PropertySpec();
                pSpecH.setType("HostSystem");
                pSpecH.getPathSet().add("name");
                fSpec.getPropSet().add(pSpecH);

                PropertySpec pSpecV = new PropertySpec();
                pSpecV.setType("VirtualMachine");
                pSpecV.getPathSet().add("name");
                fSpec.getPropSet().add(pSpecV);

                /* Place Holder. This property spec did not return cluster name for Datastore and ResourcePool.
                PropertySpec pSpecD = new PropertySpec();
                pSpecD.setType("Datastore");
                pSpecD.getPathSet().add("name");
                fSpec.getPropSet().add(pSpecD);
                PropertySpec pSpecR = new PropertySpec();
                pSpecR.setType("ResourcePool");
                pSpecR.getPathSet().add("name");
                fSpec.getPropSet().add(pSpecR);
                */
            }

            List<PropertyFilterSpec> fSpecList = new ArrayList<PropertyFilterSpec>();
            fSpecList.add(fSpec);

            RetrieveOptions ro = new RetrieveOptions();
            RetrieveResult props = connection.getVimPort().retrievePropertiesEx(propColl, fSpecList, ro);

            while((props != null) && (props.getObjects() != null) && (props.getObjects().size() > 0)){
                String token = props.getToken();
                for(ObjectContent oc : props.getObjects()){
                    List<DynamicProperty> dps = oc.getPropSet();

                    if(ClusterName != null){
                        String cluster = new String(ClusterName).replace(" ", "_");
                        String entityName = new String((String)dps.get(0).getVal()).replace(" ", "_");
                        logger.debug("ClusterName: " + cluster + " : " + oc.getObj().getType() + ": " + entityName + " : ClusterEntityMapSize: " + clusterMap.size());
                        clusterMap.put(entityName, cluster);
                    }
                    if(ClusterName == null){
                        initClusterHostMap((String) (dps.get(0).getVal()), oc.getObj(), context, clusterMap);
                    }
                }
                if (token == null) break;
                props = connection.getVimPort().continueRetrievePropertiesEx(propColl, token);
            } // while
            logger.debug("initClusterHostMap End");
            return retVal;
        } catch(Exception e){
            logger.fatal("Critical Error Detected.");
            logger.fatal(e.getLocalizedMessage());
            return false;
        }
    } // initClusterHostMap

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
        logger.debug("TST 4 - isHostMap: " + isHostMap);
        logger.debug("TST 4 - hostMap size: " + hostMap.size());
        if(isHostMap) {
            if ((hostMap.size() > 0) && (hostName != null && !hostName.equals(""))) {
                MapPrefixSuffix mapPrefixSuffix = hostMap.get(hostName);

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
}
