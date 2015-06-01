GraphiteReceiver CHANGELOG
==========================

1.0-IPM-2
---------
- Nathan Haneysmith <nathan.haneysmith@nordstrom.com> - Sample Config w Prefix

1.0-IPM-1
---------
- Nathan Haneysmith <nathan.haneysmith@nordstrom.com> - Adding customizable
  prefix via config file

1.0-IPM-3.2
-----------
- Lava Kumar <lava.a.basavapatna@nordstrom.com>
- Changes Made:
    : Modified to include cluster name in Graphite Streams. The interval for cache refreshment is configurable.
    : The GraphiteReceiver connects to multiple Graphite Nodes in the backend (via port 2003). For effective load balancing
      among multiple graphite-cyanite nodes, modified to support graphite server connection resets. This is configurable
      attribute in config/graphite.xml.
    : Look at README.md for alternative way of starting StatsFeeder server.

1.0-IPM-3.3
-----------
- Lava Kumar <lava.a.basavapatna@nordstrom.com
- Changes Made:
    : Instrumented debug logging for better performance
    : Supported Datastore, ResourcePool entities along with HostSystem, and VirtualMachine managed entities.
    : By default only HostSystem and VirtualMachine managed entities are enabled. To enable Datastore and ResourcePool entities, please add these entities
        in graphite.xml (sampleConfig.xml).
        : Replace "<childType>HostSystem,VirtualMachine</childType>" with "<childType>HostSystem,VirtualMachine,Datastore,ResourcePool</childType>" in <entities> xml node.
            Please note that no space is allowed between entities.
    : Corrected attribute names: "disconnection_graphite_after => graphite_force_reconnect_timeout", and "cache_refresh_interval => cluster_map_refresh_timeout"

