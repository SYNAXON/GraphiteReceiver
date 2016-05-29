package de.synaxon.graphitereceiver.core;

import com.vmware.ee.statsfeeder.ExecutionContext;
import com.vmware.ee.statsfeeder.MOREFRetriever;
import com.vmware.ee.statsfeeder.PerfMetricSet;
import com.vmware.ee.statsfeeder.PerfMetricSet.PerfMetric;
import com.vmware.ee.statsfeeder.StatsExecutionContextAware;
import com.vmware.ee.statsfeeder.StatsFeederListener;
import com.vmware.ee.statsfeeder.StatsListReceiver;
import de.synaxon.graphitereceiver.core.xml.ReadRules;
import de.synaxon.graphitereceiver.domain.MapPrefixSuffix;
import de.synaxon.graphitereceiver.domain.Rule;
import de.synaxon.graphitereceiver.utils.Calculate;
import de.synaxon.graphitereceiver.utils.RuleUtils;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
    private Properties props;
    private ExecutionContext context;
    //set to true for backwards compatibility
    private boolean use_fqdn;
    //set to true for backwards compatibility
    private boolean use_entity_type_prefix;
    private boolean only_one_sample_x_period;
    private boolean place_rollup_in_the_end;
    private boolean instanceMetrics;
    private boolean globalInstance;
    private int disconnectCounter;
    private int disconnectAfter;
    private boolean isHostMap;
    private Map<String, MapPrefixSuffix> hostMap;
    private Map<String,String> clusterMap = new HashMap<String, String>();
    private Socket client;
    private PrintWriter out;
    private boolean isResetConn;
    private long metricsCount;
    private int refreshClusterMapPeriod;
    private int clusterPeriod;
    private int refreshHostMapPeriod;
    private int hostMapPeriod;
    private Map<String, List<Rule>> rules;


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
     * @param props properties
     */
    public MetricsReceiver(String name, Properties props) {
        this.clusterPeriod = 0;
        this.hostMapPeriod = 0;
        this. logger = LogFactory.getLog(MetricsReceiver.class);
        this.debugLogLevel = logger.isDebugEnabled();
        this.name = (name == null) ? "SampleStatsReceiver": name;
        this.props = props;
        this.disconnectCounter = 0;
        logger.debug("MetricsReceiver Constructor.");
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

        if(this.props.getProperty("prefix") == null || this.props.getProperty("prefix").isEmpty()) {
            this.props.setProperty("prefix", "vmware");
        }

        if(this.props.getProperty("use_fqdn") != null && !this.props.getProperty("use_fqdn").isEmpty()) {
            this.use_fqdn = Boolean.valueOf(this.props.getProperty("use_fqdn"));
        } else {
            this.use_fqdn= true;
        }

        if(this.props.getProperty("use_entity_type_prefix") != null && !this.props.getProperty("use_entity_type_prefix").isEmpty()) {
            this.use_entity_type_prefix = Boolean.valueOf(this.props.getProperty("use_entity_type_prefix"));
        }

        if(this.props.getProperty("only_one_sample_x_period")!= null && !this.props.getProperty("only_one_sample_x_period").isEmpty()) {
            this.only_one_sample_x_period = Boolean.valueOf(this.props.getProperty("only_one_sample_x_period"));
        } else {
            this.only_one_sample_x_period = true;
        }

        if(this.props.getProperty("place_rollup_in_the_end") != null && !this.props.getProperty("place_rollup_in_the_end").isEmpty()) {
            this.place_rollup_in_the_end = Boolean.valueOf(this.props.getProperty("place_rollup_in_the_end"));
        }

        if(this.props.getProperty("disable_instance_metrics") != null && !this.props.getProperty("disable_instance_metrics").isEmpty()) {
            this.instanceMetrics = Boolean.valueOf(this.props.getProperty("disable_instance_metrics"));
        } else {
            this.instanceMetrics = false;
        }
        
        if(this.props.getProperty("use_global_instance") != null && !this.props.getProperty("use_global_instance").isEmpty()) {
            this.globalInstance = Boolean.valueOf(this.props.getProperty("use_global_instance"));
        } else {
            this.globalInstance = false;
        }

        boolean isRules = false;
        if(this.props.getProperty("names_transformation_rules") != null && !this.props.getProperty("names_transformation_rules").isEmpty()) {
            isRules = Boolean.valueOf(this.props.getProperty("names_transformation_rules"));
        }

        if(isRules){
            String path = null;
            if(this.props.getProperty("names_transformation_rules_path") != null && !this.props.getProperty("names_transformation_rules_path").isEmpty()) {
                path = this.props.getProperty("names_transformation_rules_path");
            }
            if(path != null) {
                ReadRules readRules = new ReadRules(path);
                this.rules = readRules.getRules();
            }
        }

        try{
            this.disconnectAfter = Integer.parseInt(this.props.getProperty("graphite_force_reconnect_timeout"));
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

        long frequencyInSeconds = this.context.getConfiguration().getFrequencyInSeconds();
        this.refreshClusterMapPeriod = Utils.calculateIteration(frequencyInSeconds, this.props.getProperty("cluster_map_refresh_timeout"), "cluster_map_refresh_timeout");
        this.refreshHostMapPeriod = Utils.calculateIteration(frequencyInSeconds, this.props.getProperty("alternate_vm_prefix_sufix_timeout"), "alternate_vm_prefix_sufix_timeout");

        if(this.props.getProperty("use_alternate_vm_prefix_sufix") != null && !this.props.getProperty("use_alternate_vm_prefix_sufix").isEmpty()) {
            this.isHostMap = Boolean.valueOf(this.props.getProperty("use_alternate_vm_prefix_sufix"));
        }
        if(this.isHostMap) {
            String hostMapPath = this.props.getProperty("alternate_vm_prefix_sufix_map_file");
            if (hostMapPath != null && !hostMapPath.equals("")) {
                try {
                    MapperPrefixSuffix mapperPrefixSuffix = new MapperPrefixSuffix(hostMapPath);
                    this.hostMap = mapperPrefixSuffix.getAllMapper();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        Utils.initClusterHostMap(null, null, this.context, this.clusterMap);
        logger.debug("MetricsReceiver  setExecutionContext.");
    }

    /**
     * This method is guaranteed to be called at the start of each retrieval in single or feeder mode.
     * Receivers can place initialization code here that should be executed before retrieval is started.
     */
    @Override
    public void onStartRetrieval() {
        this.clusterPeriod++;
        this.hostMapPeriod++;
        if(this.refreshClusterMapPeriod <= this.clusterPeriod){
            logger.debug("refreshClusterMapPeriod at period: " + this.clusterPeriod);
            this.refreshClusterMapPeriod();
        }
        if(this.isHostMap && (this.refreshHostMapPeriod <= this.hostMapPeriod)){
            logger.debug("refreshHostMapPeriod at period: " + this.hostMapPeriod);
            this.refreshHostMapPeriod();
        }
        try {
            logger.debug("onStartRetrieval - Graphite Host and Port: " + this.props.getProperty("host") + "\t" + this.props.getProperty("port"));
            this.disconnectCounter = 0;

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

    public void refreshClusterMapPeriod() {
        Utils.initClusterHostMap(null, null, this.context, this.clusterMap);
        this.clusterPeriod = 0;
    }

    public void refreshHostMapPeriod() {
        String hostMapPath = this.props.getProperty("alternate_vm_prefix_sufix_map_file");
        if (hostMapPath != null && !hostMapPath.equals("")) {
            try {
                MapperPrefixSuffix mapperPrefixSuffix = new MapperPrefixSuffix(hostMapPath);
                this.hostMap = mapperPrefixSuffix.getAllMapper();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        this.hostMapPeriod = 0;
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
            if(!isResetConn){
                logger.info("onEndRetrieval PerformanceMetricsCountForEachRun: " + metricsCount);
            }
            this.out.close();
            this.client.close();

        } catch (IOException ex) {
            logger.error("Can't close resources.", ex);
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
            String entityNameParsed = "";
            logger.debug("MetricsReceiver in receiveStats");
            if (metricSet != null) {
                String cluster = null;

                if((metricSet.getEntityName().contains("VirtualMachine")) || (metricSet.getEntityName().contains("HostSystem"))){

                    entityNameParsed = morefRetriever.parseEntityName(metricSet.getEntityName());

                    if(entityNameParsed.equals("")){
                        logger.warn("Received Invalid Managed Entity. Failed to Continue.");
                        return;
                    }
                    cluster = String.valueOf(clusterMap.get(entityNameParsed.replace(" ", "_")));
                    if(cluster == null || cluster.equals("")){
                        logger.warn("Cluster Not Found for Entity " + entityNameParsed.replace(" ", "_"));
                        return;
                    }
                    logger.debug("Cluster and Entity: " + cluster + " : " + entityNameParsed.replace(" ", "_"));
                }

                String instanceName = (this.rules.get("instanceName") != null)? RuleUtils.applyRules(metricSet.getInstanceId(),this.rules.get("instanceName")):metricSet.getInstanceId();
                
                if( ( this.globalInstance == true ) && ( instanceName == null || instanceName.isEmpty() ) )
                {
                        instanceName = "global";
                }

                String statType=metricSet.getStatType();


                int interval=metricSet.getInterval();

                String rollup;
                String hostName = null;
                if(entityName.contains("[VirtualMachine]")) {
                    hostName = (this.rules.get("hostName") != null)?RuleUtils.applyRules(entityNameParsed,this.rules.get("hostName")):entityNameParsed;
                }

                String eName = Utils.getEName(this.use_entity_type_prefix, this.use_fqdn, entityName, entityNameParsed, this.rules.get("eName"));
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
                    cluster = (this.rules.get("cluster") != null)?RuleUtils.applyRules(cluster,this.rules.get("cluster")):cluster;

                }

                String[] counterInfo = Utils.splitCounterName(metricSet.getCounterName());
                String groupName = counterInfo[0];
                String metricName = counterInfo[1];
                rollup = counterInfo[2];

                Map<String,String> graphiteTree = new HashMap<String, String>();
                graphiteTree.put("graphite_prefix", this.props.getProperty("prefix"));
                graphiteTree.put("cluster", cluster); //
                graphiteTree.put("eName", eName); //
                graphiteTree.put("groupName", groupName);
                graphiteTree.put("instanceName", instanceName); //
                graphiteTree.put("metricName", metricName);
                graphiteTree.put("statType", statType);
                graphiteTree.put("rollup", rollup);
                graphiteTree.put("counterName", metricSet.getCounterName());
                graphiteTree.put("hostName", hostName); //

                String node = Utils.getNode(graphiteTree, place_rollup_in_the_end, this.isHostMap, this.hostMap);

                metricsCount += metricSet.size();
                if(node != null) {
                    if(this.instanceMetrics) {
                        if(instanceName == null || instanceName.isEmpty()) {
                            this.sendMetric(metricSet, node, rollup);
                        }
                    } else {
                        this.sendMetric(metricSet, node, rollup);
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
                    this.props.getProperty("host") + "\t" +
                    Integer.parseInt(this.props.getProperty("port", "2003")));
            System.exit(-1);
        }
    }

    private void sendMetric(PerfMetricSet metricSet, String node, String rollup){
        Integer frequencyInSeconds = this.context.getConfiguration().getFrequencyInSeconds();
        if (only_one_sample_x_period) {
            logger.debug("one sample x period");
            int itv = metricSet.getInterval();
            if (frequencyInSeconds % itv != 0) {
                logger.warn("frequency " + frequencyInSeconds + " is not multiple of interval: " + itv + " at metric : " + node);
                return;
            }
            this.sendMetric(node, metricSet.getMetrics(), rollup);

        } else {
            logger.debug("all samples");
            sendAllMetrics(node, metricSet);
        }
    }

    private void sendMetric(String node,Iterator<PerfMetric> metrics, String rollup){
        try {
            String value = "";
            if (rollup.equals("average")) {
                value = Calculate.average(metrics);
            } else if (rollup.equals("latest")) {
                value = Calculate.latest(metrics);
            } else if (rollup.equals("maximum")) {
                value = Calculate.maximun(metrics);
            } else if (rollup.equals("minimum")) {
                value = Calculate.minimun(metrics);
            } else if (rollup.equals("summation")) {

                value = Calculate.sumation(metrics);
            } else {
                logger.info("Not supported Rollup agration:" + rollup);
            }
            if((this.disconnectAfter > 0) && (++this.disconnectCounter >= this.disconnectAfter)){
                logger.debug("sendMetric - PerfMetric Counter Value: " + this.disconnectCounter);
                this.resetGraphiteConnection();
            }
            if(node != null && node.contains("_percent_")) {
                value = scalePercent(value);
            }
            out.printf("%s %s%n", node, value);
            if(this.debugLogLevel){
                String str = String.format("%s %s", node, value);
                logger.debug("Graphite Output Summation: " + str);
            }
        } catch (ParseException t) {
            logger.error("Error processing entity stats on metric: "+node, t);
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

                if(this.debugLogLevel){
                    String str = String.format("%s %s %s%n", node, sample.getValue(), SDF.parse(sample.getTimestamp()).getTime() / 1000);
                    logger.debug("Graphite Output: " + str);
                }
            }
        } catch (Throwable t) {
            logger.error("Error processing entity stats on metric: " + node, t);
        }
    }

    /**
     *
     * @return receiver name
     */
    public String getName() {
        this.logger.debug("MetricsReceiver getName: " + this.name);
        return name;
    }

    public String scalePercent(String node){
        String[] split = node.split(" ");
        double scale = Double.valueOf(split[0]) / 100;
        return scale + " " + split[1];
    }
}
