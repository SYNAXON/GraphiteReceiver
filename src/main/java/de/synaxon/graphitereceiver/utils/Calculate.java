package de.synaxon.graphitereceiver.utils;


import com.vmware.ee.statsfeeder.PerfMetricSet.PerfMetric;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.TimeZone;

public class Calculate {

    private static final Log logger = LogFactory.getLog(Calculate.class);
    private static final DateFormat SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public static String average(Iterator<PerfMetric> metrics) throws ParseException {
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        int size = 0;
        double value = 0;
        String timestamp = "";
        while(metrics.hasNext()){
            PerfMetric perfMetric = metrics.next();
            double next = Double.valueOf(perfMetric.getValue());
            value += next;
            timestamp = perfMetric.getTimestamp();
            size++;
        }
        return String.valueOf(value/size) + " " + (SDF.parse(timestamp).getTime() / 1000);
    }

    public static String latest(Iterator<PerfMetric> metrics) throws ParseException {
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        String value = "";
        String timestamp = "";
        while (metrics.hasNext()) {
            PerfMetric perfMetric = metrics.next();
            value = perfMetric.getValue();
            timestamp = perfMetric.getTimestamp();
        }
        return value + " " + (SDF.parse(timestamp).getTime() / 1000);
    }

    public static String maximun(Iterator<PerfMetric> metrics) throws ParseException {
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        double value = 0;
        String timestamp = "";
        while (metrics.hasNext()) {
            PerfMetric perfMetric  = metrics.next();
            double last= Double.valueOf(perfMetric.getValue());
            if(last > value) {
                value = last;
            }
            timestamp = perfMetric.getTimestamp();
        }
        return String.valueOf(value) + " " + (SDF.parse(timestamp).getTime() / 1000);
    }

    public static String minimun(Iterator<PerfMetric> metrics) throws ParseException {
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        double value = 0;
        String timestamp = "";
        while (metrics.hasNext()) {
            PerfMetric perfMetric = metrics.next();
            double last= Double.valueOf(perfMetric.getValue());
            if(last < value) {
                value=last;
            }
            timestamp = perfMetric.getTimestamp();
        }
        return String.valueOf(value) + " " + (SDF.parse(timestamp).getTime() / 1000);
    }

    public static String sumation(Iterator<PerfMetric> metrics) throws ParseException {
        SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        double value = 0;
        String timestamp = "";
        while(metrics.hasNext()){
            PerfMetric perfMetric = metrics.next();
            double next = Double.valueOf(perfMetric.getValue());
            timestamp = perfMetric.getTimestamp();
            value += next;
        }
        return String.valueOf(value) + " " + (SDF.parse(timestamp).getTime() / 1000);
    }
}
