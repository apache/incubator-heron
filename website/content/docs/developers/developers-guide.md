# Topology Developers Guide

Heron provides a [Storm](http://storm.apache.org/) compatibility layer that allows existing Storm topologies to be launched on Heron without having to do any code changes. This document serves as a guide to develop and deploy topologies using Storm API on Heron. Developers should be aware of [Storm API (TODO: Add link)](), [Heron Overview (TODO: Add link)]() to follow through this guide. 

## Maven Heron Dependency

To use a specific [version (TODO: Add link)]() of storm-compatibility library, add the following dependency into your pom file:

        <dependency>
          <groupId>com.twitter.heron</groupId>
          <artifactId>heron-storm-compatibility</artifactId>
          <version>version_number</version>
        </dependency>


## Heron Examples

If you don't have an existing topology, download and try out the [Heron Examples (TODO: Add link)]()

## Deploying a topology

In order to deploy a topology, [heron client (TODO: add link)]() that we provide must be installed in the developer's local machine. Heron client has a bunch of commands that allow the launching, restarting and killing of a topology. Developer can specify a particular scheduler (local, aurora, mesos etc.) to be picked for deployment.

#### Local Mode

TODO

#### Local Scheduler

Local scheduler allows the developer to launch all the spouts, bolts and other components of the topology as separate processes withing the host machine to allow for easier debugging and faster iterations cycles before being rolled out to distributed enviroments.

Compile the topology with the storm-compatibility dependency as mentioned above. Use the generated binary jar to deploy using the following command:

        heron submit <options> local <topology binary jar> <topology command-line arguments>

To kill the topology:

        heron kill <options> local <topology name>

The `local` option suggests the heron client to pick the Local Scheduler.

**Note**: In local scheduler mode, make sure that the number of workers and parallelism of components are small enough to not exceed the resource usage on the local machine. This mode is only intended for debugging purposes.

#### Aurora Scheduler

TODO



