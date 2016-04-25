# Topology Developers Guide

Heron provides a [Storm](http://storm.apache.org/) compatibility layer that
allows existing Storm topologies to Heron without having to do any code
changes. This document serves as a guide to develop and deploy topologies
using Storm API on Heron.

## Maven Heron Dependency

To use a specific [version](TODO: Add release-notes link) of Heron
compatibility API directly, add the following dependency into your
pom file:

        <dependency>
          <groupId>com.twitter.heron</groupId>
          <artifactId>heron-storm-compatibility</artifactId>
          <version>version_number</version>
        </dependency>


## Heron Examples

If you don't have an existing topology, download and try out the
[Heron Examples](TODO: Add a link)

## Deploying a topology

In order to deploy a topology, [heron-cli](TODO: Link) that we provide must be
installed in the developer's local machine. heron-cli has a bunch of commands
that (are well documented and) allow the launching, restarting and killing of
a topology. Developer can specify a particular scheduler (local, aurora, mesos etc.)
to be picked for deployment.

### Local Scheduler

Local scheduler allows the developer to launch all the spouts, bolts and other
components of the topology as separate processes withing the host machine to allow
for easier debugging and faster iterations cycles before being rolled out to distributed
enviroments.


