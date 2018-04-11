---
title: Implementing a Bolt
---

{{< alert "spouts-and-bolts" >}}

Bolts must implement the [`IBolt`](/api/org/apache/heron/api/bolt/IBolt.html) interface.

```java
public interface IBolt extends Serializable {
  void prepare(Map<String, Object> heronConf, TopologyContext context, OutputCollector collector);
  void execute(Tuple input);
  void cleanup();
}
```

* The `prepare` method is called when the bolt is first initialized and provides
the bolt with the executing environment.

* The `execute` method is called to process a single input `Tuple`. The `Tuple`
contains metadata about component/stream/task it comes from. And `OutputCollector`
is used to emit the result.

* The `cleanup` method is called before the bolt is shutdown. There's no
guarantee that this method is called due to how the instance is killed.

See [`ExclamationBolt`](https://github.com/apache/incubator-heron/blob/master/examples/src/java/org/apache/heron/examples/api/ExclamationTopology.java#L85) for a simple bolt example.

Instead of implementing the [`IBolt`](/api/org/apache/heron/api/bolt/IBolt.html) interface directly, you can implement [`IRichBolt`](/api/org/apache/heron/api/bolt/IRichBolt.html).
