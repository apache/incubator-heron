# System-level Configuration



## General

Config | Meaning | Default
:----- |:------- |:-------
`heron.java.home.path` | The `JAVA_HOME` path on the host's environment | TODO

## Logging

Config | Meaning | Default
:----- |:------- |:-------
`heron.logging.directory` | The relative path to the logging directory | `log-files`
`heron.logging.maximum.size.mb` | The maximum log file size (in megabytes) | 100
`heron.logging.maximum.files` | The maximum number of log files | 5
`heron.logging.prune.interval.sec` | The time interval, in seconds, at which Heron prunes log files | 300
`heron.logging.flush.interval.sec` | The time interval, in seconds, at which Heron flushes log files | 10
`heron.logging.err.threshold` | The threshold level to log error | 3
