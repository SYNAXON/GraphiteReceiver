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
            node = String.format(
                    "monitoring.nagios.%s.%s_%s",
                    metricSet.getEntityName().replace("[vCenter]", "").replace("[VirtualMachine]", "").replace("[HostSystem]", "").replace('.', '_').replace('-', '_').replace(' ', '_'),
                    metricSet.getCounterName(),
                    metricSet.getStatType()
            );
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
