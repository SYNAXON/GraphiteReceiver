package de.synaxon.graphitereceiver;

import com.vmware.ee.statsfeeder.ExecutionContext;
import com.vmware.ee.statsfeeder.PerfMetricSet;
import com.vmware.ee.statsfeeder.PerfMetricSet.PerfMetric;
import com.vmware.ee.statsfeeder.StatsExecutionContextAware;
import com.vmware.ee.statsfeeder.StatsFeederListener;
import com.vmware.ee.statsfeeder.StatsListReceiver;
import com.vmware.ee.statsfeeder.MOREFRetriever;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.vmware.vim25.*;
import com.vmware.ee.common.VimConnection;
import java.util.*;

/**
 *
 * @author karl spies
 */
public class MetricsReceiver implements StatsListReceiver,
        StatsFeederListener, StatsExecutionContextAware {

    Log logger = LogFactory.getLog(MetricsReceiver.class);

    private String name = "SampleStatsReceiver";
    private String graphite_prefix = "vmware";
    //set to true for backwards compatibility
    private Boolean use_fqdn = true; 
    //set to true for backwards compatibility
    private Boolean use_entity_type_prefix = false;
    private Boolean only_one_sample_x_period=true;

    private ExecutionContext context;
    private PrintStream writer;
    private Socket client;
    PrintWriter out;
    private Properties props;
    private int freq;
    private MOREFRetriever mor;
    
    private int disconnectCounter = 0;
    private int disconnectAfter = -1;
    private boolean isResetConn = false;
    private long metricsCount = 0;
    private boolean isClusterHostMapInitialized = false;
    private Map clusterMap = Collections.EMPTY_MAP;
    private int cacheRefreshInterval = -1;
    private Date cacheRefreshStartTime = null;

    /**
     * This constructor will be called by StatsFeeder to load this receiver. The props object passed is built
     * from the content in the XML configuration for the receiver.
     * <pre>
     *    {@code
     *    <receiver>
     *      <name>sample</name>
     *      <class>com.vmware.ee.statsfeeder.SampleStatsReceiver</class>
     *      <!-- If you need some properties specify them like this
     *      <properties>
     *          <property>
     *              <name>some_property</name>
     *              <value>some_value</value>
     *          </property>
     *      </properties>
     *      -->
     *    </receiver>
     *    }
     * </pre>
     *
     * @param name
     * @param props
     */
    public MetricsReceiver(String name, Properties props) {
        this.name = name;
        this.props = props;
        logger.debug("MetricsReceiver Constructor.");
    }

    /**
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
        this.logger.debug("MetricsReceiver setName: " + this.name);
    }

    /**
     *
     * @return
     */
    public String getName() {
        this.logger.debug("MetricsReceiver getName: " + this.name);
        return name;
    }

    /**
     * This method is called when the receiver is initialized and passes the StatsFeeder execution context
     * which can be used to look up properties or other configuration data.
     *
     * It can also be used to retrieve the vCenter connection information
     *
     * @param context - The current execution context
     */
    @Override
    public void setExecutionContext(ExecutionContext context) {
        
        logger.debug("MetricsReceiver in setExecutionContext.");
        this.context = context;
        this.mor=context.getMorefRetriever();
        this.freq=context.getConfiguration().getFrequencyInSeconds();
        
        String prefix=this.props.getProperty("prefix");
        String use_fqdn=this.props.getProperty("use_fqdn");
        String use_entity_type_prefix=this.props.getProperty("use_entity_type_prefix");
        String only_one_sample_x_period=this.props.getProperty("only_one_sample_x_period");
        
        if(prefix != null && !prefix.isEmpty())
            this.graphite_prefix=prefix;
        
        if(use_fqdn != null && !use_fqdn.isEmpty())
            this.use_fqdn=Boolean.valueOf(use_fqdn);
        
        if(use_entity_type_prefix != null && !use_entity_type_prefix.isEmpty())
            this.use_entity_type_prefix=Boolean.valueOf(use_entity_type_prefix);
        
        if(only_one_sample_x_period!= null && !only_one_sample_x_period.isEmpty())
            this.only_one_sample_x_period=Boolean.valueOf(only_one_sample_x_period);

        try{
            this.disconnectAfter = Integer.parseInt(this.props.getProperty("disconnection_graphite_after"));
            if(this.disconnectAfter < 1){
                logger.info("if disconnection_graphite_after is set to < 1 will not be supported: " + this.disconnectAfter);
                this.disconnectAfter = -1;
            }else{
                logger.info("In setExecutionContext:: disconnectCounter and disconnectAfter Values: " + this.disconnectCounter + "\t" + this.disconnectAfter);
            }
        }catch(Exception e){
            logger.debug("disconnection_graphite_after attribute is not set or not supported.");
            logger.debug("disconnection_graphite_after is set to < 1 will not be supported: ");
            this.disconnectAfter = -1;
        }

        this.clusterMap = new HashMap();

        try{
            this.cacheRefreshInterval = Integer.parseInt(this.props.getProperty("cache_refresh_interval"));
            if(this.cacheRefreshInterval < 1){
                logger.info("if cache_refresh_interval is set to < 1 will not be supported: " + this.cacheRefreshInterval);
                this.cacheRefreshInterval = -1;
            }else{
                logger.info("setExecutionContext:: cache_refresh_interval Value: " + this.cacheRefreshInterval);
            }
        }catch(Exception e){
            logger.debug("cache_refresh_interval attribute is not set or not supported.");
            logger.debug("cache_refresh_interval is set to < 1 will not be supported: ");
            this.cacheRefreshInterval = -1;
        }
    }
    
    /**
     * initClusterHostMap is a self recursive method for generating VM/ESX to Cluster Hash Map.
     * In the first iteration it gathers all clusters and in consecutive calls for each cluster it updates Hash Map.
     * The logic here is use ComputeResource Entity as a base for gathering all virtual machines and ESX Hosts.
     * As part of configurations, GraphiteReceiver invokes this method at regular intervals (configured) and during runtime
     * if VM/ESX does not exist in the hash map.
     */
    public boolean initClusterHostMap(String ClusterName, ManagedObjectReference ClusterMor){
        try {
            logger.debug("initClusterHostMap Begin");
            boolean retVal = true;

            VimConnection connection = this.context.getConnection();

            ManagedObjectReference viewMgrRef = connection.getViewManager();
            ManagedObjectReference propColl = connection.getPropertyCollector();
            List<String> clusterList = new ArrayList<String>();
            clusterList.add("ComputeResource");
            clusterList.add("HostSystem");
            clusterList.add("VirtualMachine");
            ManagedObjectReference rootFolder = null;
            if(ClusterName == null){
                rootFolder = connection.getRootFolder();
                this.clusterMap.clear();
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
                        logger.debug("ClusterName: " + cluster + " : " + oc.getObj().getType() + ": " + entityName + " : ClusterEntityMapSize: " + this.clusterMap.size());
                        this.clusterMap.put(entityName, cluster);
                    }
                    if(ClusterName == null){
                        this.initClusterHostMap((String)(dps.get(0).getVal()), oc.getObj());
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

    /**
     * getCluster returns associated cluster to the calling method. If requested VM/ESX managed entity does not exist in the cache,
     * it refreshes the cache.
     *
     * Potential Bottlenecks: If too many new VirtualMachines/ESX hosts added during runtime (between cache refresh intervals - this.cacheRefreshInterval)
     *                        may affect the performance because of too many vCenter connections and cache refreshments. We have tested & verified 
     */
    private String getCluster(String entity){
        try{
            String value = (String)this.clusterMap.get(entity);
            if(value == null){
                logger.warn("Cluster Not Found for Managed Entity " + entity);
                logger.warn("Reinitializing Cluster Entity Map");
                this.initClusterHostMap(null, null);
                value = (String)this.clusterMap.get(entity);
            }
            return value;
        }catch(Exception e){
            return null;
        }
    }

    private void sendAllMetrics(String node,PerfMetricSet metricSet){
        final DateFormat SDF = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            Iterator<PerfMetric> metrics = metricSet.getMetrics();
            while (metrics.hasNext()) {
                if((this.disconnectAfter > 0) && (++this.disconnectCounter >= this.disconnectAfter)){
                    logger.debug("sendAllMetrics - PerfMetric Counter Value: " + this.disconnectCounter);
                    this.resetGraphiteConnection();
                }
                PerfMetric sample = metrics.next();
                out.printf("%s %s %s%n", node, sample.getValue(), SDF.parse(sample.getTimestamp()).getTime() / 1000);

                String str = new String(String.format("%s %s %s%n", node, sample.getValue(), SDF.parse(sample.getTimestamp()).getTime() / 1000));
                logger.debug("Graphite Output: " + str);
            }
        } catch (Throwable t) {
                logger.error("Error processing entity stats on metric: "+node, t);
        }
    }
            
    private void sendMetricsAverage(String node,PerfMetricSet metricSet,int n){
        //averaging all values with last timestamp
        final DateFormat SDF = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            double value;
            Iterator<PerfMetric> metrics = metricSet.getMetrics();
            //sample initialization
            PerfMetric sample=metrics.next();
            value=Double.valueOf(sample.getValue());  
            while (metrics.hasNext()) {
                sample = metrics.next();
                value+=Double.valueOf(sample.getValue());
            }
            if((this.disconnectAfter > 0) && (++this.disconnectCounter >= this.disconnectAfter)){
                logger.debug("sendMetricsAverage - PerfMetric Counter Value: " + this.disconnectCounter);
                this.resetGraphiteConnection();
            }       
            out.printf("%s %f %s%n", node, value/n, SDF.parse(sample.getTimestamp()).getTime() / 1000);

            String str = new String(String.format("%s %f %s%n", node, value/n, SDF.parse(sample.getTimestamp()).getTime() / 1000));
            logger.debug("Graphite Output Average: " + str);
        } catch (NumberFormatException t) {
                logger.error("Error on number format on metric: "+node, t);
        } catch (ParseException t) {
            logger.error("Error processing entity stats on metric: "+node, t);
        }
    }

    private void sendMetricsLatest(String node,PerfMetricSet metricSet) {
        final DateFormat SDF = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        try{
            //get last
            
            Iterator<PerfMetric> metrics = metricSet.getMetrics();
            PerfMetric sample=metrics.next();
            while (metrics.hasNext()) {
                sample = metrics.next();
            }   
            if((this.disconnectAfter > 0) && (++this.disconnectCounter >= this.disconnectAfter)){
                logger.debug("sendMetricsLatest - PerfMetric Counter Value: " + this.disconnectCounter);
                this.resetGraphiteConnection();
            }
            out.printf("%s %s %s%n", node,sample.getValue() , SDF.parse(sample.getTimestamp()).getTime() / 1000);  

            String str = new String(String.format("%s %s %s%n", node,sample.getValue() , SDF.parse(sample.getTimestamp()).getTime() / 1000));
            logger.debug("Graphite Output Latest: " + str);
        } catch (ParseException t) {
            logger.error("Error processing entity stats on metric: "+node, t);
        }
    }
    
    private void sendMetricsMaximum(String node,PerfMetricSet metricSet) {
        final DateFormat SDF = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            double value; 
            Iterator<PerfMetric> metrics = metricSet.getMetrics();
            //first value to compare
            PerfMetric sample=metrics.next();
            value=Double.valueOf(sample.getValue());
            //begin comparison iteration
            while (metrics.hasNext()) {
                sample = metrics.next();
                double last=Double.valueOf(sample.getValue());
                if(last > value) value=last;
            }   
            if((this.disconnectAfter > 0) && (++this.disconnectCounter >= this.disconnectAfter)){
                logger.debug("sendMetricsMaximum - PerfMetric Counter Value: " + this.disconnectCounter);
                this.resetGraphiteConnection();
            }
            out.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000); 

            String str = new String(String.format("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000));
            logger.debug("Graphite Output Maximum: " + str);
        } catch (ParseException t) {
            logger.error("Error processing entity stats on metric: "+node, t);
        }
    }

    private void sendMetricsMinimim(String node,PerfMetricSet metricSet) {
        final DateFormat SDF = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            //get minimum values with last timestamp
            double value;     
            Iterator<PerfMetric> metrics = metricSet.getMetrics();
            //first value to compare
            PerfMetric sample=metrics.next();
            value=Double.valueOf(sample.getValue());
            //begin comparison iteration
            while (metrics.hasNext()) {
                sample = metrics.next();
                double last=Double.valueOf(sample.getValue());
                if(last < value) value=last;
            }   
            if((this.disconnectAfter > 0) && (++this.disconnectCounter >= this.disconnectAfter)){
                logger.debug("sendMetricsMinimim - PerfMetric Counter Value: " + this.disconnectCounter);
                this.resetGraphiteConnection();
            }
            out.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000); 

            String str = new String(String.format("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000));
            logger.debug("Graphite Output Minimum: " + str);
        } catch (ParseException t) {
            logger.error("Error processing entity stats on metric: "+node, t);
        }
    }
    
    private void sendMetricsSummation(String node,PerfMetricSet metricSet){
        final DateFormat SDF = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
         try {
            //get minimum values with last timestamp
            double value;     
            Iterator<PerfMetric> metrics = metricSet.getMetrics();
            //first value to compare
            PerfMetric sample=metrics.next();
            value=Double.valueOf(sample.getValue());
            //begin comparison iteration
            while (metrics.hasNext()) {
                sample = metrics.next();
                value+=Double.valueOf(sample.getValue());
            }   
            if((this.disconnectAfter > 0) && (++this.disconnectCounter >= this.disconnectAfter)){
                logger.debug("sendMetricsSummation - PerfMetric Counter Value: " + this.disconnectCounter);
                this.resetGraphiteConnection();
            }
            out.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000); 

            String str = new String(String.format("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000));
            logger.debug("Graphite Output Summation: " + str);
        } catch (ParseException t) {
            logger.error("Error processing entity stats on metric: "+node, t);
        }       
    }
    
    private String[] splitCounterName(String counterName) {
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
     * Main receiver entry point. This will be called for each entity and each metric which were retrieved by
     * StatsFeeder.
     *
     * receiveStats becomes a synchronized method for synchronizing all threads. Made this decision because PrintWriter low level Socket
     * APIs are not completely thread safe. We have observed runtime crashes if all threads call receiveStats method simultaneously.
     *
     * @param entityName - The name of the statsfeeder entity being retrieved
     * @param metricSet - The set of metrics retrieved for the entity
     */
    @Override
    public synchronized void receiveStats(String entityName, PerfMetricSet metricSet) {
    try {
        logger.debug("MetricsReceiver in receiveStats");

        if(this.isClusterHostMapInitialized == false){
            if(this.cacheRefreshInterval != -1){
                this.cacheRefreshStartTime = new Date();
                logger.info("receiveStats cacheRefreshStartTime: " + cacheRefreshStartTime.toString());
            }
            this.isClusterHostMapInitialized = true;
            this.initClusterHostMap(null, null);
        }

        if (metricSet != null) {
            //-- Samples come with the following date format

            String node;
            String myEntityName = metricSet.getEntityName().replace("[vCenter]", "").replace("[VirtualMachine]", "").replace("[HostSystem]", "");
            if(myEntityName == null || myEntityName.equals("")){
                logger.warn("Received Invalid Managed Entity. Failed to Continue.");
                return;
            }
            myEntityName = myEntityName.replace(" ", "_");
            String cluster = this.getCluster(myEntityName);
            if(cluster == null || cluster.equals("")){
                logger.warn("Cluster Not Found for Entity " + myEntityName);
                return;
            }
            logger.debug("Cluster and Entity: " + cluster + " : " + myEntityName);

            String eName=null;
            String counterName=metricSet.getCounterName();
            //Get Instance Name
            String instanceName=metricSet.getInstanceId()
                                        .replace('.','_')
                                        .replace('-','_')
                                        .replace('/','.')
                                        .replace(' ','_');
            String statType=metricSet.getStatType();
            
            String container=null;
            int interval=metricSet.getInterval();
            
            String rollup=null;
            
            if(use_entity_type_prefix) {
                if(entityName.contains("[VirtualMachine]")) {
                    eName="vm."   +entityName.replace("[vCenter]", "").replace("[VirtualMachine]", "").replace('.', '_');
                }else if (entityName.contains("[HostSystem]")) {
                    //for ESX only hostname
                    if(!use_fqdn) {
                        eName="esx."  +entityName.replace("[vCenter]", "").replace("[HostSystem]", "").split("[.]",2)[0];
                    }else {
                        eName="esx."  +entityName.replace("[vCenter]", "").replace("[HostSystem]", "").replace('.', '_');
                    }
                }else if (entityName.contains("[Datastore]")) {
                    eName="dts."  +entityName.replace("[vCenter]", "").replace("[Datastore]", "").replace('.', '_');
                }else if (entityName.contains("[ResourcePool]")) {
                    eName="rp."   +entityName.replace("[vCenter]", "").replace("[ResourcePool]", "").replace('.', '_');
                }
                
            } else {
                eName=entityName.replace("[vCenter]", "")
                                .replace("[VirtualMachine]", "")
                                .replace("[HostSystem]", "")
                                .replace("[Datastore]", "")
                                .replace("[ResourcePool]", "");
                 
                if(!use_fqdn && entityName.contains("[HostSystem]")){
                    eName=eName.split("[.]",2)[0];
                }
                eName=eName.replace('.', '_');
            }
            eName=eName.replace(' ','_').replace('-','_');
            container=mor.getContainerName(eName);
            logger.debug("Container Name :" +container + " Interval: "+Integer.toString(interval)+ " Frequency :"+Integer.toString(freq));
            if (instanceName.equals("")) {
                String[] counterInfo=splitCounterName(counterName);
                String groupName    =counterInfo[0];
                String metricName   =counterInfo[1];
                rollup              =counterInfo[2];
                node = String.format("%s.%s.%s.%s.%s_%s_%s",graphite_prefix,cluster, eName,groupName,metricName,rollup,statType);
                logger.debug("GP :" +graphite_prefix+ " EN: "+eName+" CN :"+ counterName +" ST :"+statType);
            } else {
                //Get group name (xxxx) metric name (yyyy) and rollup (zzzz) 
                // from "xxxx.yyyyyy.xxxxx" on the metricName
                String[] counterInfo=splitCounterName(counterName);
                String groupName    =counterInfo[0];
                String metricName   =counterInfo[1];
                rollup              =counterInfo[2];         
                node = String.format("%s.%s.%s.%s.%s.%s_%s_%s",graphite_prefix,cluster, eName,groupName,instanceName,metricName,rollup,statType);
                logger.debug("GP :" +graphite_prefix+ " EN: "+eName+" GN :"+ groupName +" IN :"+instanceName+" MN :"+metricName+" RU"+rollup +"ST :"+statType);
            }
            metricsCount += metricSet.size();
            if(only_one_sample_x_period) {
                 logger.debug("one sample x period");
                //check if metricSet has the expected number of metrics
                int itv=metricSet.getInterval();
                if(freq % itv != 0) {
                    logger.warn("frequency "+freq+ " is not multiple of interval: "+itv+ " at metric : "+node);
                    return;
                }
                int n=freq/itv;
                /* Noticed expected and received samples never match. I think we should check for whether received samples
                 * is the multiple of expected samples? Commenting here to move forward.
                if(n != metricSet.getValues().size()){
                    logger.error("ERROR: "+n+" expected samples but got "+metricSet.getValues().size()+ "at metric :"+node);
                    return;
                }
                */
                if(rollup.equals("average")) {
                    sendMetricsAverage(node,metricSet,n);
                } else if(rollup.equals("latest")) {
                    sendMetricsLatest(node,metricSet);
                } else if(rollup.equals("maximum")) {
                    sendMetricsMaximum(node,metricSet);
                } else if(rollup.equals("minimum")) {
                    sendMetricsMinimim(node,metricSet);
                } else if(rollup.equals("summation")) {
                    sendMetricsSummation(node,metricSet);
                } else {
                    logger.info("Not supported Rollup agration:"+rollup);
                }
            } else {
                logger.debug("all samples");
                sendAllMetrics(node,metricSet);
            }
        } else {
            logger.debug("MetricsReceiver MetricSet is NULL");
        }
        } catch(Exception e){
            logger.fatal("Unexpected error occurred during metrics collection.", e);
        }
    }

    private void resetGraphiteConnection(){
        try {
            logger.debug("resetGraphiteConnection. Counter Value " + this.disconnectCounter + " Threshold Value of " + this.disconnectAfter + " reached. resetting Graphite Connection");
            isResetConn = true;
            this.onEndRetrieval();
            this.onStartRetrieval();
            isResetConn = false;
        }catch(Exception e){
            logger.fatal("Failed to Establish Graphite Server connection: " +
                             this.props.getProperty("host") + "\t" +
                             Integer.parseInt(this.props.getProperty("port", "2003")));
            System.exit(-1);
        }
    }

    /**
     * This method is guaranteed to be called at the start of each retrieval in single or feeder mode.
     * Receivers can place initialization code here that should be executed before retrieval is started.
     */
    @Override
    public void onStartRetrieval() {
        try {
            logger.debug("onStartRetrieval - Graphite Host and Port: " + this.props.getProperty("host") + "\t" + this.props.getProperty("port"));
            this.disconnectCounter = 0;

            if(isResetConn != true){
                metricsCount = 0;
                if(this.cacheRefreshStartTime != null){
                    Date cacheRefreshEndTime = new Date();
                    long refreshCacheTimeDiff = ((cacheRefreshEndTime.getTime()/1000) - (this.cacheRefreshStartTime.getTime()/1000));
                    if(refreshCacheTimeDiff >= this.cacheRefreshInterval){
                        this.isClusterHostMapInitialized = false;
                    }
                }
            }

            this.client = new Socket(
                    this.props.getProperty("host"),
                    Integer.parseInt(this.props.getProperty("port", "2003"))
            );
            OutputStream s = this.client.getOutputStream();
            this.out = new PrintWriter(s, true);
        } catch (IOException ex) {
            logger.error("Can't connect to graphite.", ex);
        }
    }

    /**
     * This method is guaranteed to be called, just once, at the end of each retrieval in single or feeder
     * mode. Receivers can place termination code here that should be executed after the retrieval is
     * completed.
     */
    @Override
    public void onEndRetrieval() {
        try {
            logger.debug("MetricsReceiver onEndRetrieval.");
            if(isResetConn != true){
                logger.info("onEndRetrieval PerformanceMetricsCountForEachRun: " + metricsCount);
            }

            this.out.close();
            this.client.close();

        } catch (IOException ex) {
            logger.error("Can't close resources.", ex);
        }
    }
}
