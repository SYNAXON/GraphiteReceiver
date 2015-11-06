package de.synaxon.graphitereceiver.core;

import com.vmware.ee.statsfeeder.ExecutionContext;
import com.vmware.ee.statsfeeder.MOREFRetriever;
import com.vmware.ee.statsfeeder.PerfMetricSet;
import com.vmware.ee.statsfeeder.PerfMetricSet.PerfMetric;
import com.vmware.ee.statsfeeder.StatsExecutionContextAware;
import com.vmware.ee.statsfeeder.StatsFeederListener;
import com.vmware.ee.statsfeeder.StatsListReceiver;
import de.synaxon.graphitereceiver.domain.MapPrefixSuffix;
import de.synaxon.graphitereceiver.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 *
 * @author karl spies
 */
public class MetricsReceiver implements StatsListReceiver, StatsFeederListener, StatsExecutionContextAware {

    private Log logger;
    private boolean debugLogLevel;

    private String name;
    private Properties properties;
    private ExecutionContext context;
    //set to true for backwards compatibility
    private boolean use_fqdn;
    //set to true for backwards compatibility
    private boolean use_entity_type_prefix;
    private boolean only_one_sample_x_period;
    private boolean place_rollup_in_the_end;

    private int disconnectCounter;
    private int disconnectAfter;
    private boolean isHostMap;
    private Map<String, MapPrefixSuffix> hostMap;
    private Map<String,String> clusterMap = new HashMap<String, String>();


    private Socket socket;
    private PrintWriter writer;

    private int freq;



    private boolean isResetConn;
    private long metricsCount;

    //Next PR
    private boolean isClusterHostMapInitialized = false;
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
     * @param name receiver name
     * @param properties app properties
     */
    public MetricsReceiver(String name, Properties properties) {
        this. logger = LogFactory.getLog(MetricsReceiver.class);
        this.debugLogLevel = logger.isDebugEnabled();
        this.name = (name == null) ? "SampleStatsReceiver": name;
        this.properties = properties;
        this.disconnectCounter = 0;
        logger.debug("MetricsReceiver Constructor.");
    }

    /**
     *
     * @return receive name
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
        logger.debug("MetricsReceiver join in setExecutionContext.");
        logger.debug("Getting context and properties");
        this.context = context;

        if(this.properties.getProperty("prefix") == null || this.properties.getProperty("prefix").isEmpty()) {
            this.properties.setProperty("prefix", "vmware");
        }
        if(this.properties.getProperty("use_fqdn") != null && !this.properties.getProperty("use_fqdn").isEmpty()) {
            this.use_fqdn = Boolean.valueOf(this.properties.getProperty("use_fqdn"));
        } else {
            this.use_fqdn= true;
        }
        if(this.properties.getProperty("use_entity_type_prefix") != null && !this.properties.getProperty("use_entity_type_prefix").isEmpty()) {
            this.use_entity_type_prefix = Boolean.valueOf(this.properties.getProperty("use_entity_type_prefix"));
        }

        if(this.properties.getProperty("only_one_sample_x_period")!= null && !this.properties.getProperty("only_one_sample_x_period").isEmpty()) {
            this.only_one_sample_x_period = Boolean.valueOf(this.properties.getProperty("only_one_sample_x_period"));
        } else {
            this.only_one_sample_x_period = true;
        }

        if(this.properties.getProperty("place_rollup_in_the_end") != null && !this.properties.getProperty("place_rollup_in_the_end").isEmpty()) {
            this.place_rollup_in_the_end = Boolean.valueOf(this.properties.getProperty("place_rollup_in_the_end"));
        }

        try{
            this.disconnectAfter = Integer.parseInt(this.properties.getProperty("graphite_force_reconnect_timeout"));
            if(this.disconnectAfter < 1){
                logger.info("if graphite_force_reconnect_timeout is set to < 1 will not be supported: " + this.disconnectAfter);
                this.disconnectAfter = -1;
            }else{
                logger.info("In setExecutionContext:: disconnectCounter and disconnectAfter Values: " + this.disconnectCounter + "\t" + this.disconnectAfter);
            }
        }catch(Exception e){
            logger.debug("graphite_force_reconnect_timeout attribute is not set or not supported.");
            logger.debug("graphite_force_reconnect_timeout is set to < 1 will not be supported: ");
            this.disconnectAfter = -1;
        }

        this.freq=context.getConfiguration().getFrequencyInSeconds();
        try{
            this.cacheRefreshInterval = Integer.parseInt(this.properties.getProperty("cluster_map_refresh_timeout"));
            if(this.cacheRefreshInterval < 1){
                logger.info("if cluster_map_refresh_timeout is set to < 1 will not be supported: " + this.cacheRefreshInterval);
                this.cacheRefreshInterval = -1;
            }else{
                logger.info("setExecutionContext:: cluster_map_refresh_timeout Value: " + this.cacheRefreshInterval);
            }
        }catch(Exception e){
            logger.debug("cluster_map_refresh_timeout attribute is not set or not supported.");
            logger.debug("cluster_map_refresh_timeout is set to < 1 will not be supported: ");
            this.cacheRefreshInterval = -1;
        }
        if(this.properties.getProperty("use_alternate_vm_prefix_sufix") != null && !this.properties.getProperty("use_alternate_vm_prefix_sufix").isEmpty()) {
            this.isHostMap = Boolean.valueOf(this.properties.getProperty("use_alternate_vm_prefix_sufix"));
        }
        if(this.isHostMap) {
            String hostMapPath = this.properties.getProperty("alternate_vm_prefix_sufix_map_file");
            if (hostMapPath != null && !hostMapPath.equals("")) {
                try {
                    MapperPrefixSuffix mapperPrefixSuffix = new MapperPrefixSuffix(hostMapPath);
                    this.hostMap = mapperPrefixSuffix.getAllMapper();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
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
                writer.printf("%s %s %s%n", node, sample.getValue(), SDF.parse(sample.getTimestamp()).getTime() / 1000);

                if(this.debugLogLevel == true){
                    String str = new String(String.format("%s %s %s%n", node, sample.getValue(), SDF.parse(sample.getTimestamp()).getTime() / 1000));
                    logger.debug("Graphite Output: " + str);
                }
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
            writer.printf("%s %f %s%n", node, value/n, SDF.parse(sample.getTimestamp()).getTime() / 1000);

            if(this.debugLogLevel == true){
                String str = new String(String.format("%s %f %s%n", node, value/n, SDF.parse(sample.getTimestamp()).getTime() / 1000));
                logger.debug("Graphite Output Average: " + str);
            }
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
            writer.printf("%s %s %s%n", node,sample.getValue() , SDF.parse(sample.getTimestamp()).getTime() / 1000);

            if(this.debugLogLevel == true){
                String str = new String(String.format("%s %s %s%n", node,sample.getValue() , SDF.parse(sample.getTimestamp()).getTime() / 1000));
                logger.debug("Graphite Output Latest: " + str);
            }
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
            writer.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000);

            if(this.debugLogLevel == true){
                String str = new String(String.format("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000));
                logger.debug("Graphite Output Maximum: " + str);
            }
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
            writer.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000);

            if(this.debugLogLevel == true){
                String str = new String(String.format("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000));
                logger.debug("Graphite Output Minimum: " + str);
            }
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
            writer.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000);

            if(this.debugLogLevel == true){
                String str = new String(String.format("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000));
                logger.debug("Graphite Output Summation: " + str);
            }
        } catch (ParseException t) {
            logger.error("Error processing entity stats on metric: "+node, t);
        }
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
        MOREFRetriever morefRetriever = this.context.getMorefRetriever();
        Integer frequencyInSeconds = this.context.getConfiguration().getFrequencyInSeconds();
        try {
            logger.debug("MetricsReceiver in receiveStats");

            if(this.isClusterHostMapInitialized == false){
                if(this.cacheRefreshInterval != -1){
                    this.cacheRefreshStartTime = new Date();
                    logger.info("receiveStats cacheRefreshStartTime: " + cacheRefreshStartTime.toString());
                }
                this.isClusterHostMapInitialized = true;
                Utils.initClusterHostMap(null, null, this.context, this.clusterMap);
            }

            if (metricSet != null) {
                //-- Samples come with the following date format
                String cluster = null;

                if((metricSet.getEntityName().contains("VirtualMachine") == true) || (metricSet.getEntityName().contains("HostSystem") == true)){

                    String myEntityName = morefRetriever.parseEntityName(metricSet.getEntityName());

                    if(myEntityName == null || myEntityName.equals("")){
                        logger.warn("Received Invalid Managed Entity. Failed to Continue.");
                        return;
                    }
                    myEntityName = myEntityName.replace(" ", "_");
                    cluster = String.valueOf(clusterMap.get(myEntityName));
                    if(cluster == null || cluster.equals("")){
                        logger.warn("Cluster Not Found for Entity " + myEntityName);
                        return;
                    }
                    logger.debug("Cluster and Entity: " + cluster + " : " + myEntityName);
                }

                String eName=null;
                String counterName=metricSet.getCounterName();
                //Get Instance Name
                String instanceName=metricSet.getInstanceId()
                        .replace('.','_')
                        .replace('-','_')
                        .replace('/','.')
                        .replace(' ','_');
                String statType=metricSet.getStatType();


                int interval=metricSet.getInterval();

                String rollup;

                String hostName = null;
                if(use_entity_type_prefix) {
                    if(entityName.contains("[VirtualMachine]")) {
                        hostName = morefRetriever.parseEntityName(entityName).replace('.', '_');
                        eName="vm." + hostName;
                    }else if (entityName.contains("[HostSystem]")) {
                        //for ESX only hostname
                        if(!use_fqdn) {
                            eName = "esx."+morefRetriever.parseEntityName(entityName).split("[.]",2)[0];
                        }else {
                            eName = "esx."+morefRetriever.parseEntityName(entityName).replace('.', '_');
                        }
                    }else if (entityName.contains("[Datastore]")) {
                        eName = "dts."+morefRetriever.parseEntityName(entityName).replace('.', '_');
                    }else if (entityName.contains("[ResourcePool]")) {
                        eName = "rp."+morefRetriever.parseEntityName(entityName).replace('.', '_');
                    }

                } else {
                    eName = morefRetriever.parseEntityName(entityName);
                    if(!use_fqdn && entityName.contains("[HostSystem]")){
                        eName = eName.split("[.]",2)[0];
                    }
                    eName = eName.replace('.', '_');
                }
                if(eName != null) {
                    eName = eName.replace(' ', '_').replace('-', '_');
                }
                logger.debug("Container Name :" + morefRetriever.getContainerName(eName) + " Interval: "+Integer.toString(interval)+ " Frequency :"+Integer.toString(frequencyInSeconds));

                /*
                    Finally node contains these fields (depending on properties)
                    graphite_prefix.cluster.eName.groupName.instanceName.metricName_rollup_statType
                    graphite_prefix.cluster.eName.groupName.instanceName.metricName_statType_rollup
                    NOTES: if cluster is null cluster name disappears from node string.
                           if instanceName is null instanceName name disappears from node string.
                 */
                //Get group name (xxxx) metric name (yyyy) and rollup (zzzz)
                // from "xxxx.yyyyyy.xxxxx" on the metricName
                if(cluster != null) {
                    cluster = cluster.replace(".", "_");
                }

                String[] counterInfo = Utils.splitCounterName(counterName);
                String groupName = counterInfo[0];
                String metricName = counterInfo[1];
                rollup = counterInfo[2];

                Map<String,String> graphiteTree = new HashMap<String, String>();
                graphiteTree.put("graphite_prefix", this.properties.getProperty("prefix"));
                graphiteTree.put("cluster", cluster);
                graphiteTree.put("eName", eName);
                graphiteTree.put("groupName", groupName);
                graphiteTree.put("instanceName", instanceName);
                graphiteTree.put("metricName", metricName);
                graphiteTree.put("statType", statType);
                graphiteTree.put("rollup", rollup);
                graphiteTree.put("counterName", counterName);
                graphiteTree.put("hostName", hostName);
                String node = Utils.getNode(graphiteTree, place_rollup_in_the_end, this.isHostMap, this.hostMap);
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
                    if(node != null) {
                        if (rollup.equals("average")) {
                            sendMetricsAverage(node, metricSet, n);
                        } else if (rollup.equals("latest")) {
                            sendMetricsLatest(node, metricSet);
                        } else if (rollup.equals("maximum")) {
                            sendMetricsMaximum(node, metricSet);
                        } else if (rollup.equals("minimum")) {
                            sendMetricsMinimim(node, metricSet);
                        } else if (rollup.equals("summation")) {
                            sendMetricsSummation(node, metricSet);
                        } else {
                            logger.info("Not supported Rollup agration:" + rollup);
                        }
                    }
                } else {
                    if(node != null) {
                        logger.debug("all samples");
                        sendAllMetrics(node, metricSet);
                    }
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
                    this.properties.getProperty("host") + "\t" +
                    Integer.parseInt(this.properties.getProperty("port", "2003")));
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
            this.refreshHostMapPeriod();
            logger.debug("onStartRetrieval - Graphite Host and Port: " + this.properties.getProperty("host") + "\t" + this.properties.getProperty("port"));
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

            this.socket = new Socket(
                    this.properties.getProperty("host"),
                    Integer.parseInt(this.properties.getProperty("port", "2003"))
            );
            OutputStream s = this.socket.getOutputStream();
            this.writer = new PrintWriter(s, true);
        } catch (IOException ex) {
            logger.error("Can't connect to graphite.", ex);
        }
    }

    public void refreshHostMapPeriod() {
        String hostMapPath = this.properties.getProperty("alternate_vm_prefix_sufix_map_file");
        if (hostMapPath != null && !hostMapPath.equals("")) {
            try {
                MapperPrefixSuffix mapperPrefixSuffix = new MapperPrefixSuffix(hostMapPath);
                this.hostMap = mapperPrefixSuffix.getAllMapper();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        //Next PR
        //this.hostMapPeriod = 0;
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

            this.writer.close();
            this.socket.close();

        } catch (IOException ex) {
            logger.error("Can't close resources.", ex);
        }
    }
}
