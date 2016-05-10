## Heron UI Usage Guide

### Overview

This guide helps in understanding how to make best use of Heron UI for
monitoring and debugging your topologies.

The UI enables a user to get a lot of information about a topology or a part of
topology quickly, thus reducing the debugging time considerably. Some of these
features are listed below. Complete set of features can be found in next few
sections.

1. See logical plan of a topology
2. See physical plan of a topology
3. Configs of a topology
3. See some basic metrics for each of the instances and components
4. Links to get logs, memory histogram, jstack, heapdump and exceptions of
   a particular instance

and other useful links and information, that are explained below.

#### Topologies Page

`TODO: Add home page image`

This is the home page of Heron UI.

Following information can be found on this page.

1. List of all topologies
2. Number of topologies fitered after search (total by default)
2. A topology's overview
3. Link to a topology's configuration

Following actions can be taken on this page.

1. Filter the topologies using `cluster`
2. Filter the topologies using string matching in names, clusters, environs,
   roles, versions, or submitters
3. Sort the topologies based on a particular column
4. Click on the topology name to find more info about the topology
5. Click on the `Config` button to look at the configurations for the topology

#### Topology Page

`TODO: Add topology page image`

This is the main page to monitor a topology.
