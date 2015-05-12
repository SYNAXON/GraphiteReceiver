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

