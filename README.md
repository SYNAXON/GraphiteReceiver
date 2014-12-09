Einleitung
==========
Diese Plugin arbeitet mit dem VMWare StatsFeeder zusammen. Dieser StatsFeeder
liest die Metriken aus dem vCenter oder aus einem VMWare Host aus und schreibt
sie direkt in das Graphite.

Aufruf
======
```
C:\home\spies\Projects\Java\StatsFeeder>StatsFeeder.bat -h vcenter.zentrale.de -u ZENTRALE\Administrator -p ....-c config/sampleConfig.xml
```

Links
=====
https://labs.vmware.com/flings/statsfeeder