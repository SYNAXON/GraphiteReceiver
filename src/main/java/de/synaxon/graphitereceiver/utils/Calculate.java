package de.synaxon.graphitereceiver.utils;


import com.vmware.ee.statsfeeder.PerfMetricSet.PerfMetric;

import java.util.Iterator;

public class Calculate {
    public static double calculateAverage(Iterator<PerfMetric> metrics, int n){
        PerfMetric perfMetric = metrics.next();
        double value = Double.valueOf(perfMetric.getValue());
        while (metrics.hasNext()) {
            perfMetric = metrics.next();
            value+=Double.valueOf(perfMetric.getValue());
        }
        return value/n;
    }

    public static double calculateLatest(Iterator<PerfMetric> metrics){
        PerfMetric perfMetric = metrics.next();
        while (metrics.hasNext()) {
            perfMetric = metrics.next();
        }
        return Double.valueOf(perfMetric.getValue());
    }

    public static double calculateMaximun(Iterator<PerfMetric> metrics){
        PerfMetric perfMetric = metrics.next();
        double value = Double.valueOf(perfMetric.getValue());
        //begin comparison iteration
        while (metrics.hasNext()) {
            perfMetric = metrics.next();
            double last=Double.valueOf(perfMetric.getValue());
            if(last > value) {
                value = last;
            }
        }
        return value;
    }

    public static double calculateMinimun(Iterator<PerfMetric> metrics){
        PerfMetric perfMetric = metrics.next();
        double value = Double.valueOf(perfMetric.getValue());
        //begin comparison iteration
        while (metrics.hasNext()) {
            perfMetric = metrics.next();
            double last=Double.valueOf(perfMetric.getValue());
            if(last < value) {
                value=last;
            }
        }
        return value;
    }
    public static double calculateSumation(Iterator<PerfMetric> metrics){
        PerfMetric perfMetric = metrics.next();
        double value = Double.valueOf(perfMetric.getValue());
        //begin comparison iteration
        while (metrics.hasNext()) {
            perfMetric = metrics.next();
            value+=Double.valueOf(perfMetric.getValue());
        }
        return value;
    }
}
