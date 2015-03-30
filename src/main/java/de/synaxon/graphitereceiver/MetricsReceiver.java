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
    }
    
    private void sendAllMetrics(String node,PerfMetricSet metricSet){
        final DateFormat SDF = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            Iterator<PerfMetric> metrics = metricSet.getMetrics();
            while (metrics.hasNext()) {
                PerfMetric sample = metrics.next();
                out.printf("%s %s %s%n", node, sample.getValue(), SDF.parse(sample.getTimestamp()).getTime() / 1000);
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
            out.printf("%s %f %s%n", node, value/n, SDF.parse(sample.getTimestamp()).getTime() / 1000);
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
            out.printf("%s %s %s%n", node,sample.getValue() , SDF.parse(sample.getTimestamp()).getTime() / 1000);  
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
            out.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000); 
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
            out.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000); 
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
            out.printf("%s %f %s%n", node, value, SDF.parse(sample.getTimestamp()).getTime() / 1000); 
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
     * @param entityName - The name of the statsfeeder entity being retrieved
     * @param metricSet - The set of metrics retrieved for the entity
     */
    @Override
    public void receiveStats(String entityName, PerfMetricSet metricSet) {
        if (metricSet != null) {
            //-- Samples come with the following date format

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
                node = String.format("%s.%s.%s.%s_%s_%s",graphite_prefix,eName,groupName,metricName,rollup,statType);
                logger.debug("GP :" +graphite_prefix+ " EN: "+eName+" CN :"+ counterName +" ST :"+statType);
            } else {
                //Get group name (xxxx) metric name (yyyy) and rollup (zzzz) 
                // from "xxxx.yyyyyy.xxxxx" on the metricName
                String[] counterInfo=splitCounterName(counterName);
                String groupName    =counterInfo[0];
                String metricName   =counterInfo[1];
                rollup              =counterInfo[2];         
                node = String.format("%s.%s.%s.%s.%s_%s_%s",graphite_prefix,eName,groupName,instanceName,metricName,rollup,statType);
                logger.debug("GP :" +graphite_prefix+ " EN: "+eName+" GN :"+ groupName +" IN :"+instanceName+" MN :"+metricName+" RU"+rollup +"ST :"+statType);
            }

            if(only_one_sample_x_period) {
                 logger.debug("one sample x period");
                //check if metricSet has the expected number of metrics
                int itv=metricSet.getInterval();
                if(freq % itv != 0) {
                    logger.warn("frequency "+freq+ " is not multiple of interval: "+itv+ " at metric : "+node);
                    return;
                }
                int n=freq/itv;
                if(n != metricSet.getValues().size()){
                    logger.error("ERROR: "+n+" expected samples but got "+metricSet.getValues().size()+ "at metric :"+node);
                    return;
                }
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
