---
title: Setting Up Local File System State Manager
---

Heron can use the local file system as a state manager for storing various book 
keeping information. Use of local file system is recommended mainly for single
node server and laptop. This configuration is ideal for deploying in edge devices. 
Heron developers can use this setting for developing and debugging various heron 
components in their laptop or server.

### Local File System State Manager Configuration

You can make Heron aware of the ZooKeeper cluster by modifying the
`statemgr.yaml` config file specific for the Heron cluster. You'll
need to specify the following for each cluster:

* `heron.class.state.manager` &mdash; Indicates the class to be loaded for local file system 
state manager. You should set this to `com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager`

* `heron.statemgr.connection.string` &mdash; This should be `LOCALMODE` since it always localhost. 

* `heron.statemgr.root.path` &mdash; The root path in the local file system where state information 
is stored.  We recommend providing Heron with an exclusive directory; if you do not, make sure that 
the following sub-directories are unused: `/tmasters`, `/topologies`, `/pplans`, `/executionstate`, 
`/schedulers`.

* `heron.statemgr.localfs.is.initialize.file.tree` &mdash; Indicates whether the nodes under root 
`/tmasters`, `/topologies`, `/pplans`, `/executionstate`, and `/schedulers` need to created, if they 
are not found. Set it to `True`, if you could like Heron to create those directories. If those 
directories are already there, set it to `False`. The absence of this configuration implies `True`.

### Example Local File System State Manager Configuration

Below is an example configuration (in `statemgr.yaml`) for a local file system running in `localhost`:

<pre><code>
# local state manager class for managing state in a persistent fashion
heron.class.state.manager: com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager

# local state manager connection string
heron.statemgr.connection.string: LOCALMODE

# path of the root address to store the state in a local file system
heron.statemgr.root.path: ${HOME}/.herondata/repository/state/${CLUSTER}

# create the sub directories, if needed
heron.statemgr.localfs.is.initialize.file.tree: True
</code></pre>
