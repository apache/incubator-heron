---
title: Implementing a Spout
---

Spouts must implement the [`ISpout`](/api/com/twitter/heron/api/spout/ISpout.html) interface.

```java
public interface ISpout extends Serializable {
  void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector);
  void close();
  void activate();
  void deactivate();
  void nextTuple();
  void ack(Object msgId);
  void fail(Object msgId);
}
```

* The `open` method is called when the spout is initialized and provides the
spout with the executing environment.

* The `close` method is called when the spout is shutdown. There's no guarantee
that this method is called due to how the instance is killed.

* The `activate` method is called when the spout is asked to back into active
state.

* The `deactivate` method is called when the spout is asked to enter deactive
state.

* The `nextTuple` method is used to fetch tuples from input source and emit it
to [`OutputCollector`](/api/com/twitter/heron/api/bolt/).

* The `ack` method is called when the `Tuple` with the `msgId` emitted by this
spout is successfully processed.

* The `fail` method is called when the `Tuple` with the `msgId` emitted by this
spout is not processed successfully.

See [`TestWordSpout`](https://github.com/twitter/heron/blob/master/heron/examples/src/java/com/twitter/heron/examples/TestWordSpout.java) for a simple spout example.

Instead of implementing the [`ISpout`](/api/com/twitter/heron/api/spout/ISpout.html) interface directly, you can implement [`IRichSpout`](/api/com/twitter/heron/api/spout/IRichSpout.html).
