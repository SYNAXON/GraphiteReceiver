package de.synaxon.graphitereceiver;

import com.vmware.ee.statsfeeder.ExecutionContext;
import com.vmware.ee.statsfeeder.PerfMetricSet;
import com.vmware.ee.statsfeeder.PerfMetricSet.PerfMetric;
import com.vmware.ee.statsfeeder.StatsExecutionContextAware;
import com.vmware.ee.statsfeeder.StatsFeederListener;
import com.vmware.ee.statsfeeder.StatsListReceiver;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private ExecutionContext context;
    private PrintStream writer;
    private Socket client;
    PrintWriter out;
    private Properties props;

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
    }

    /**
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @return
     */
    public String getName() {
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
        this.context = context;
        String prefix=this.props.getProperty("prefix");
        String use_fqdn=this.props.getProperty("use_fqdn");
        String use_entity_type_prefix=this.props.getProperty("use_entity_type_prefix");
        
        if(prefix != null && !prefix.isEmpty())
            this.graphite_prefix=prefix;
        
        if(use_fqdn != null && !use_fqdn.isEmpty())
            this.use_fqdn=Boolean.valueOf(use_fqdn);
        
        if(use_entity_type_prefix != null && !use_entity_type_prefix.isEmpty())
            this.use_entity_type_prefix=Boolean.valueOf(use_entity_type_prefix);
    }

    /**
     * Main receiver entry point. This will be called for each entity and each metric which were retrieved by
     * StatsFeeder.
     *
     * @param entityName - The name of the statsfeeder entity being retrieved
     * @param metricSet - The set of metrics retrieved for the entity
     */
    @Override
    public void receiveStats(String entityName, PerfMetricSet metricSet) {
        if (metricSet != null) {
            //-- Samples come with the following date format
            final DateFormat SDF = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
            SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
            String node;
            
            String eName=null;
            String counterName=metricSet.getCounterName();
            //Get Instance Name
            String instanceName=metricSet.getInstanceId()
                                        .replace('.','_')
                                        .replace('-','_')
                                        .replace('/','.')
                                        .replace(' ','_');
            String statType=metricSet.getStatType();
            
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
            
            if (instanceName.equals("")) {
                node = String.format("%s.%s.%s_%s",graphite_prefix,eName,counterName,statType);
                logger.debug("GP :" +graphite_prefix+ " EN: "+eName+" CN :"+ counterName +" ST :"+statType);
            } else {
                //Get group name (xxxx) metric name (yyyy) and rollup (zzzz) 
                // from "xxxx.yyyyyy.xxxxx" on the metricName
                String groupName=counterName.split("[.]",2)[0];
                String metricName=counterName.split("[.]",2)[1];               
                node = String.format("%s.%s.%s.%s.%s_%s",graphite_prefix,eName,groupName,instanceName,metricName,statType);
                logger.debug("GP :" +graphite_prefix+ " EN: "+eName+" GN :"+ groupName +" IN :"+instanceName+" MN :"+metricName+" ST :"+statType);
            }
            
            //logger.debug("ENTITY NAME : " + entityName  +" ENTITY NAME2: "+metricSet.getEntityName()+ " COUNTER NAME: " + metricSet.getCounterName());
            //logger.debug("MOREF : "+metricSet.getMoRef()+ " INSTANCE ID :"+ metricSet.getInstanceId()+" string :"+metricSet.toString());
            try {
                Iterator<PerfMetric> metrics = metricSet.getMetrics();
                while (metrics.hasNext()) {
                    PerfMetric sample = metrics.next();
                    out.printf("%s %s %s%n", node, sample.getValue(), SDF.parse(sample.getTimestamp()).getTime() / 1000);
                }
            } catch (Throwable t) {
                logger.error("Error processing entity stats ", t);
            }
        }
    }

    /**
     * This method is guaranteed to be called at the start of each retrieval in single or feeder mode.
     * Receivers can place initialization code here that should be executed before retrieval is started.
     */
    @Override
    public void onStartRetrieval() {
        try {
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
            this.out.close();
            this.client.close();

        } catch (IOException ex) {
            logger.error("Can't close resources.", ex);
        }
    }
}
