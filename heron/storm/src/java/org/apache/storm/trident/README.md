Hackweek prototype to get a sample trident topology working.

tl;dr; Heron does not support direct grouping, which Trident requires. To get Trident to work, we
need to either support direct grouping in Heron, or refactor Trident to not require it (if that's
possible).

To run:

```
$ ~/bin/heron kill local TridentWordCountTopology && rm -rf ~/.herondata/*
$ bazel run --config=darwin --verbose_failures -- scripts/packages/heron-client-install.sh --user && \
  ~/bin/heron submit local ~/.heron/examples/heron-examples.jar \
  com.twitter.heron.examples.TridentWordCountTopology TridentWordCountTopology
$ less ~/.herondata/topologies/local/billg/TridentWordCountTopology/log-files/container_1_b-1_4.log.0
```

Current status:
- Topology compiles and can be submitted
- DAG and streams appears to be correctly represented
- Tuple routing is incorrect due to hacks (see below)
- Trident/Storm do not provide reasonable defaults to configs and instances fails violently when
  expected configs are not set. See TridentWordCountTopology.
- Many methods have been added/hacked to get the topology to run, but
- Failures appear in the counters for bolt b-1, but the logs don't show anything and
- Correctness is not right due to the following

Issues:
1. Direct grouping needs to be implemented, currently hacking using shuffle grouping (see grouping.cpp)
2. `com.twitter.heron.instance.bolt.BoltOutputCollectorImpl.emitDirect` not supported and hacked to just emit
3. `com.twitter.heron.instance.bolt.BoltOutputCollectorImpl.admitBoltTuple` changed to return task ids
4. `BoltDeclarerImpl.grouping(GlobalStreamId id, Grouping grouping)` doesn't support `CUSTOM_SERIALIZED` properly
5. GeneralTopologyContext does a bunch of janky stuff with NoValueMap, for callers who need keySets only
6. Zookeeper acls are not implemented in Utils.

TODO:
- Figure out why bolt b-1 is failing. This is likely because tuples are being mis-routed due to the
  grouping hacks
- Understand why direct grouping and `emitDirect` are needed and how to support, remove hacks (see #1, #2 above)
- Fix `admitBoltTuple` changed to return task ids to return real tuples ids (see #3 above)
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
