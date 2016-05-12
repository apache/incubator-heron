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
4. See some basic metrics for each of the instances and components
5. Links to get logs, memory histogram, jstack, heapdump and exceptions of
   a particular instance

and other useful links and information, that are explained below.

#### Topologies Page

`TODO: Add home page image`

This is the home page of Heron UI.

Following information or actions can be found on this page.

1. List of all topologies
2. Number of topologies fitered after search (total by default)
3. A topology's overview
4. Filter the topologies using `cluster`
5. Filter the topologies using string matching in names, clusters, environs,
   roles, versions, or submitters
6. Sort the topologies based on a particular column
7. Click on the topology name to find more info about the topology

#### Topology Page

`TODO: Add topology page image`

This is the main page to monitor a topology. There are some basic metrics that
can be found here, and some generic info. A complete list of information and
actions is described below.

1. Name of the topology
2. Logical plan `(TODO: Link to what is logical plan)` of the topology
3. Physical plan `(TODO: Link to what is physical plan)` of the topology
4. Health metrics for the topology
5. General Info about the topology
6. General metrics for the topology
7. Click components for more details
8. Click instances for more details

Each node in logical plan can be clicked for more specific info about that
component. `TODO: Add topology page with component clicked`

1. Link to topology level configs
2. Link to Job page  only if the scheduler provides a link
   `(TODO: Link to this configuration)`
3. Link to Viz dashboard for this topology only if Tracker is configured with
   one. `(TODO: Link to this configuration)`
4. Averaged or Max metrics for all instances of this component
5. Aggregated metrics for all instances of this component
6. List of all instances and their aggregated metrics
7. Instance level operations, which are described in more details below

Clicking on an instance will highlight that instance in the list.
`TODO: Add topology page with instance clicked`

1. Aggregated metrics are only for this instance
2. Component counters are still aggregated for all instances
3. The selected instance is highlighted
