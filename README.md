GraphiteReceiver
================
A vmware statsfeeder receiver to export the directly into graphite.

Sample Config
-------------
A detailed config can be found in the directory sample.

```
<receiver>
    <name>graphite</name>
    <class>de.synaxon.graphitereceiver.MetricsReceiver</class>
    <properties>
        <!--  Set the file name. Files will be rolled over on the hour -->
        <property>
            <name>host</name>
            <value>graphite server</value>
        </property>
        <property>
            <name>port</name>
            <value>2003</value>
        </property>
    </properties>
</receiver>
```

Development
-----------
You need to install the dependencies into your local mvn repository via mvn install!
```
mvn install:install-file -DgroupId=com.vmware.tools -Dversion=4.1 -Dpackaging=jar -DgeneratePom=true -DartifactId=statsfeeder-common -Dfile=statsfeeder-common-4.1.jar
mvn install:install-file -DgroupId=com.vmware.tools -Dversion=4.1 -Dpackaging=jar -DgeneratePom=true -DartifactId=statsfeeder-core -Dfile=statsfeeder-core-4.1.jar
```

Call
----
```
StatsFeeder.bat -h vcenter -u user -p password -c config/sampleConfig.xml
```

Links
-----
https://labs.vmware.com/flings/statsfeeder
