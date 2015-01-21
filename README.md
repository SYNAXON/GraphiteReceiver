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

Build JAR (on Linux)
--------------------
 # Make sure you have JDK installed and not just JRE.  Maybe with apt-get install openjdk-7-jdk
 wget http://download3.vmware.com/software/vmw-tools/statsfeeder/StatsFeeder-4.1.697.zip
 mkdir statsfeeder
 cd statsfeeder
 unzip ../StatsFeeder-4.1.697.zip
 cd ..
 git clone https://github.com/SYNAXON/GraphiteReceiver.git
 cd GraphiteReceiver
 mkdir -p ~/.m2/repository/com/vmware/tools/statsfeeder-common/4.1
 mkdir -p ~/.m2/repository/com/vmware/tools/statsfeeder-core/4.1
 cp ../statsfeeder/lib/statsfeeder-common-4.1.jar ~/.m2/repository/com/vmware/tools/statsfeeder-common/4.1
 cp ../statsfeeder/lib/statsfeeder-core-4.1.jar ~/.m2/repository/com/vmware/tools/statsfeeder-core/4.1
 # Install Maven :)  Maybe with apt-get install maven
 mvn package
 cp target/GraphiteReceiver-1.0-SNAPSHOT.jar ../statsfeeder/lib
 cd ../statsfeeder
 # Run StatsFeeder as you normally would but ../GraphiteReceiver/sampleConfig.xml modified to point at your Graphite (or InfluxDB) server

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
