## Building Bolts

To implement a bolt, you need to imlement the `IBolt` interface to tell what the bolt should do.
```java
public interface IBolt extends Serializable {
	void prepare(Map<String, Object> heronConf, TopologyContext context, OutputCollector collector);
	void execute(Tuple input);
	void cleanup();
}
```
The `prepare` method is called when the bolt is first initialized and provides the bolt with the executing environment.

The `execute` method is called to process a single input `Tuple`. The `Tuple` contains metadata about component/stream/task it comes from. And `OutputCollector` is used to emit the result.

The `cleanup` method is called before the bolt is shutdown. But there's no guarantee that this method is called due to how the instance is killed.

A simple Bolt example is the `TestWordBolt`

```java
  public static class ExclamationBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1184860508880121352L;
    private long nItems;
    private long startTime;

    @Override
    public void prepare(
        Map<String, Object> conf,
        TopologyContext context,
        OutputCollector collector) {
      nItems = 0;
      startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
      collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

```

And when you actually develop topology in Java, the `IRichBolt` should be used as the main interface. 
