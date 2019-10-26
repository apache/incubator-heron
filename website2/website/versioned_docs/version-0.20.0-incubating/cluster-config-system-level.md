---
id: version-0.20.0-incubating-cluster-config-system-level
title: System Level Configuration
sidebar_label: System Level Configuration
original_id: cluster-config-system-level
---
<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

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