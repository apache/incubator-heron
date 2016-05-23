## Building Spouts

To implement a spout, you need to implement the `ISpout` interface to tell what the spout should do.

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

The `open` method is called when the spout is initialized and provides the spout with the executing environment.

The `close` method is called when the spout is shutdown. But there's no guarantee that this method is called due to how the instance is killed.

The `activate` method is called when the spout is asked to back into active state.

The `deactivate` method is called when the spout is asked to enter deactive state.

The `nextTuple` method is used to fetch tuples from input source and emit it to `OutputCollector`.

The `ack` method is called when the `Tuple` with the `msgId` emitted by this spout is successfully processed.

The `fail` method is called when the `Tuple` with the `msgId` emitted by this spout is not processed successfully.

A simple Spout example is the `TestWordSpout`
```java
public class TestWordSpout extends BaseRichSpout {

  private static final long serialVersionUID = -6923231632469169744L;
  private SpoutOutputCollector collector;
  private String[] words;
  private Random rand;

  public void open(
      Map<String, Object> conf,
      TopologyContext context,
      SpoutOutputCollector aCollector) {
    collector = aCollector;
    words = new String[]{"Africa", "Europe", "Asia", "America", "Antarctica", "Australia"};
    rand = new Random();
  }

  public void close() {
  }

  public void nextTuple() {
    final String word = words[rand.nextInt(words.length)];
    collector.emit(new Values(word));
  }

  public void ack(Object msgId) {
  }

  public void fail(Object msgId) {
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}

```

And when you actually develop topology in Java, the `IRichSpout` should be used as the main interface.
