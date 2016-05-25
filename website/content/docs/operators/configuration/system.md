---
title: System-level Configuration
---

The parameters in the sections below are set at the system level and thus do not
apply to any specific component.

## General

Config | Meaning | Default
:----- |:------- |:-------
`heron.check.tmaster.location.interval.sec` | The interval, in seconds, after which to check if the topology master location has been fetched or not | 120
`heron.metrics.export.interval` | The interval, in seconds, at which components export metrics to the topology's Metrics Manager

## Logging

Config | Meaning | Default
:----- |:------- |:-------
`heron.logging.directory` | The relative path to the logging directory | `log-files`
`heron.logging.maximum.size.mb` | The maximum log file size (in megabytes) | 100
`heron.logging.maximum.files` | The maximum number of log files | 5
`heron.logging.prune.interval.sec` | The time interval, in seconds, at which Heron prunes log files | 300
`heron.logging.flush.interval.sec` | The time interval, in seconds, at which Heron flushes log files | 10
`heron.logging.err.threshold` | The threshold level to log error | 3
