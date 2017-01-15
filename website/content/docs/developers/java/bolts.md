---
title: Implementing a Bolt
---

Bolts must implement the [`IBolt`](/api/com/twitter/heron/api/bolt/IBolt.html) interface.

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

See [`ExclamationBolt`](https://github.com/twitter/heron/blob/master/heron/examples/src/java/com/twitter/heron/examples/ExclamationTopology.java#L67) for a simple bolt example.

Instead of implementing the [`IBolt`](/api/com/twitter/heron/api/bolt/IBolt.html) interface directly, you can implement [`IRichBolt`](/api/com/twitter/heron/api/bolt/IRichBolt.html).
