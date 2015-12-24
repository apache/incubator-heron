# Heron Logging

You can configure Heron's logging system, including a target logging directory,
error threshold, and more.

## Configurable Parameters

Config | Meaning
:----- |:-------
`heron.logging.directory` | The relative path to the logging directory
`heron.logging.maximum.size.mb` | The maximum log file size (in megabytes)
`heron.logging.maximum.files` | The maximum number of log files
`heron.logging.prune.interval.sec` | The time interval, in seconds, at which Heron prunes log files
`heron.logging.flush.interval.sec` | The time interval, in seconds, at which Heron flushes log files
`heron.logging.err.threshold` | The threshold level to log error
