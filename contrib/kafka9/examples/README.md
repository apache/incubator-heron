# Launching example topologies

Currently it is possible to launch topologies under Vagrant, as well as on the arbitrary cluster. For instructions on 
running topologies on Vagrant cluster please refer the corresponding readme: `contrib/kafka9/vagrant/README.md`
In order to run Heron on a cluster, one should have the appropriate `package` of the topology to run. This readme 
focuses on how to build it 

## Pre-requisites

To build the topology package one should have:

- Built Heron release package
- A topology packed in a jar executable
- Heron CLI extracted from the release package and set up the proper way

In order to build Heron *and* the example topologies simply run the following from the project root dir:
 
```
# CentOS 7
./build-centos.sh

# Ubuntu 14.04
./build-ubuntu.sh
```

If you have Heron built and want to build *only* the topology jars, execute from this dir:

```
./build-topologies.sh
```

**Note:** The topologies jar files are placed to `dist/topologies` dir. If one wants a new topology to be built by 
launching these commands, it would be necessary to include building that topology with Docker image. Please refer
to `docker/compile.sh` for details.

The last thing to do, if not done already, is to setup Heron CLI dir. In order to do this, run from the root dir:

```
# CentOS 7
./setup-cli-centos.sh

# Ubuntu 14.04
./setup-cli-ubuntu.sh
```

## Packaging

Topology package contains the following:

- Topology executable jar
- `.defn` file, which is a binary representation of a Protobuf entity containing Heron-specific configurations such as 
topology name, initial state, etc.
- Heron internals config file

One may package the topology using the following command launched from this dir:

```
./pack-topology.sh <jar_file_name> <main_class_name> <topology_name> <args>
```

**NOTE:** It is essential to specify the same topology name which one will use to actually launch the topology. Also
keep in mind that if Heron configs depend on some command arguments, they should be provided here as well.

Find your package by the path `dist/packages/dc/role/environ/<topology_name>/topology.tar.gz`, upload it to any location
 reachable from your cluster and specify the link to it in your launch configs.