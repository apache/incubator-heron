Hackweek prototype to get a sample trident topology working.

tl;dr; TridentWordCountTopologyHeron runs and tuples are being transmitted. Bolt b-1 receives
sentances and splits them into words and counts. Bolt b-0 does not seem to receive and aggregate
them. We need to understand why.

To run:

```
$ cd path/to/zookeeper && bin/zkServer.sh start
$ ~/bin/heron kill local TridentWordCountTopology && rm -rf ~/.herondata/*
$ bazel run --config=darwin --verbose_failures -- scripts/packages/heron-client-install.sh --user && \
  ~/bin/heron submit local ~/.heron/examples/heron-examples.jar \
  com.twitter.heron.examples.TridentWordCountTopology TridentWordCountTopology
$ less ~/.herondata/topologies/local/billg/TridentWordCountTopology/log-files/container_1_b-1_4.log.0
```

Current status:
- Topology compiles and can be submitted
- DAG and streams appears to be correctly represented
- Aggregation does not appear to be happening on the terminal bolt (b-0)
- Trident/Storm do not provide reasonable defaults to configs and instances fails violently when
  expected configs are not set. See TridentWordCountTopology.
- Many methods have been added/hacked to get the topology to run, but
- Failures on stream $coord-bg0 appear in the counters for bolt b-1, but the logs don't show anything

Issues:
1. `com.twitter.heron.instance.bolt.BoltOutputCollectorImpl.admitBoltTuple` changed to return task ids
2. `BoltDeclarerImpl.grouping(GlobalStreamId id, Grouping grouping)` doesn't support `CUSTOM_SERIALIZED` properly
3. GeneralTopologyContext does a bunch of janky stuff with NoValueMap, for callers who need keySets only
4. Zookeeper acls are not implemented in Utils.

TODO:
- Figure out why bolt b-1 is failing to process tuples on the $coord-bg0 stream.
- Bolt b-1 seems to receive sentences on stream s1 and split them into words in the code, but they
don't seem to be getting to b-0. Understand why. Are they being emitted and received and he counters
are wrong, or are they not emitted.
- Understand MemoryMapState and see counts getting persisted in it. I suspect this should be done by b-0.
- Understand why direct grouping and `emitDirect` are needed
- Fix `admitBoltTuple` changed to return task ids to return real tuples ids (see #1 above)
- Understand why `CUSTOM_SERIALIZED` is needed and how to support (see #4 above)
- Figure out why `org.apache.storm.trident.topology.TransactionAttempt` is only registered as
  `Config.TOPOLOGY_KRYO_REGISTER` in spouts and not bolts.
- Lots of additions were added to org.apache.storm code in heron. These implementations should all
  be verified and in some cases fixed. Storm code in heron also seems to drift between versions. This
  code should really be pinned to a given storm version. To make it easier to upgrade to new storm
  version. Because storm classes are copied and modified, managing storm versions with heron
  modifications is really hard currently.
- To get things working the org.apache.storm core jar was added as a dep to the project (see WORKSPACE),
  since it has all the trident code. We wouldn't want to do this in the long haul. Instead we'd
  probably want to include an artifact that only has the trident code.
- Do a FULL AUDIT of all changes in the branch before even thinking about merging any of it. This is
  prototype hackweek code, people.
