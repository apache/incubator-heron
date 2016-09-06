Kafka Spout
====================

Provides spout implementations for consuming data from Apache Kafka 0.8.x. Trident kafka spout and
kafka-bolt is not supported.

##Spouts
For spout implementation, we use a BrokerHost interface that tracks Kafka broker host to partition 
mapping and kafkaConfig that controls some Kafka related parameters.

###BrokerHosts
In order to initialize your Kafka spout/emitter you need to construct an instance of the marker 
interface BrokerHosts. Currently, we support the following two implementations:

####ZkHosts
ZkHosts is what you should use if you want to dynamically track Kafka broker to partition mapping. 
This class uses Kafka's ZooKeeper entries to track brokerHost -> partition mapping. 
You can instantiate an object by calling
```java
    public ZkHosts(String brokerZkStr, String brokerZkPath)
    public ZkHosts(String brokerZkStr)
```
Where brokerZkStr is just ip:port (e.g. localhost:2181). brokerZkPath is the root directory under 
which all the topics and partition information is stored. By default this is /brokers which is what 
the default Kafka implementation uses.

By default, the broker-partition mapping is refreshed every 60 seconds from ZooKeeper. If you want 
to change it, you should set host.refreshFreqSecs to your chosen value.

####StaticHosts
This is an alternative implementation where broker -> partition information is static. In order to 
construct an instance of this class, you need to first construct an instance of 
GlobalPartitionInformation.

```java
    Broker brokerForPartition0 = new Broker("localhost");//localhost:9092
    Broker brokerForPartition1 = new Broker("localhost", 9092);//localhost:9092 but we specified the port explicitly
    Broker brokerForPartition2 = new Broker("localhost:9092");//localhost:9092 specified as one string.
    GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
    partitionInfo.addPartition(0, brokerForPartition0);//mapping from partition 0 to brokerForPartition0
    partitionInfo.addPartition(1, brokerForPartition1);//mapping from partition 1 to brokerForPartition1
    partitionInfo.addPartition(2, brokerForPartition2);//mapping from partition 2 to brokerForPartition2
    StaticHosts hosts = new StaticHosts(partitionInfo);
```

###KafkaConfig
The second thing needed for constructing a kafkaSpout is an instance of KafkaConfig.
```java
    public KafkaConfig(BrokerHosts hosts, String topic)
    public KafkaConfig(BrokerHosts hosts, String topic, String clientId)
```

The BrokerHosts can be any implementation of BrokerHosts interface as described above. The topic is 
name of Kafka topic. The optional ClientId is used as a part of the ZooKeeper path where the 
spout's current consumption offset is stored.

There are 2 extensions of KafkaConfig currently in use.

Spoutconfig is an extension of KafkaConfig that supports additional fields with ZooKeeper connection
info and for controlling behavior specific to KafkaSpout. The Zkroot will be used as root to store 
your consumer's offset. The id should uniquely identify your spout.
```java
public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id);
```
In addition to these parameters, SpoutConfig contains the following fields that control how 
KafkaSpout behaves:
```java
    // setting for how often to save the current Kafka offset to ZooKeeper
    public long stateUpdateIntervalMs = 2000;

    // Retry strategy for failed messages
    public String failedMsgRetryManagerClass = ExponentialBackoffMsgRetryManager.class.getName();

    // Exponential back-off retry settings.  These are used by ExponentialBackoffMsgRetryManager for retrying messages after a bolt
    // calls OutputCollector.fail(). These come into effect only if ExponentialBackoffMsgRetryManager is being used.
    // Initial delay between successive retries
    public long retryInitialDelayMs = 0;
    public double retryDelayMultiplier = 1.0;
    
    // Maximum delay between successive retries    
    public long retryDelayMaxMs = 60 * 1000;
    // Failed message will be retried infinitely if retryLimit is less than zero. 
    public int retryLimit = -1;     

```
Core KafkaSpout only accepts an instance of SpoutConfig.


The KafkaConfig class also has bunch of public variables that controls your application's behavior. 
Here are defaults:
```java
    public int fetchSizeBytes = 1024 * 1024;
    public int socketTimeoutMs = 10000;
    public int fetchMaxWait = 10000;
    public int bufferSizeBytes = 1024 * 1024;
    public MultiScheme scheme = new RawMultiScheme();
    public boolean ignoreZkOffsets = false;
    public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
    public long maxOffsetBehind = Long.MAX_VALUE;
    public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
    public int metricsTimeBucketSizeInSecs = 60;
```

Most of them are self explanatory except MultiScheme.
###MultiScheme
MultiScheme is an interface that dictates how the byte array consumed from Kafka gets transformed 
into a storm tuple. It also controls the naming of your output field.

```java
  public Iterable<List<Object>> deserialize(ByteBuffer ser);
  public Fields getOutputFields();
```

The name of the outputField is "bytes". There are alternative implementations like
`SchemeAsMultiScheme` and `KeyValueSchemeAsMultiScheme` which can convert the `ByteBuffer` to
`String`.

There is also an extension of `SchemeAsMultiScheme`, `MessageMetadataSchemeAsMultiScheme`,
which has an additional deserialize method that accepts the message in addition to the
`Partition` and `offset` associated with the message.

```java
public Iterable<List<Object>> deserializeMessageWithMetadata(byte[] message, Partition partition, long offset)

```

This is useful for auditing/replaying messages from arbitrary points on a Kafka topic, saving the
partition and offset of each message of a discrete stream instead of persisting the entire message.

###Failed message retry
FailedMsgRetryManager is an interface which defines the retry strategy for a failed message. 
Default implementation is ExponentialBackoffMsgRetryManager which retries with exponential delays
between consecutive retries. To use a custom implementation, set 
SpoutConfig.failedMsgRetryManagerClass to the full classname of implementation. Here is the 
interface 

```java
    // Spout initialization can go here. This can be called multiple times during lifecycle of a worker. 
    void prepare(SpoutConfig spoutConfig, Map stormConf);

    // Message corresponding to offset has failed. This method is called only if retryFurther returns true for offset.
    void failed(Long offset);

    // Message corresponding to offset has been acked.  
    void acked(Long offset);

    // Message corresponding to the offset, has been re-emitted and under transit.
    void retryStarted(Long offset);

    /**
     * The offset of message, which is to be re-emitted. Spout will fetch messages starting from this offset
     * and resend them, except completed messages.
     */
    Long nextFailedMessageToRetry();

    /**
     * @return True if the message corresponding to the offset should be emitted NOW. False otherwise.
     */
    boolean shouldReEmitMsg(Long offset);

    /**
     * Spout will clean up the state for this offset if false is returned. If retryFurther is set to true,
     * spout will called failed(offset) in next call and acked(offset) otherwise 
     */
    boolean retryFurther(Long offset);

    /**
     * Clear any offsets before kafkaOffset. These offsets are no longer available in kafka.
     */
    Set<Long> clearOffsetsBefore(Long kafkaOffset);
``` 

#### Version incompatibility
In Storm versions prior to 1.0, the MultiScheme methods accepted a `byte[]` instead of `ByteBuffer`.
The `MultScheme` and the related Scheme apis were changed in version 1.0 to accept a ByteBuffer 
instead of a byte[]. But heron still uses `byte[]`.

### Examples

#### Core Spout

```java
BrokerHosts hosts = new ZkHosts(zkConnString);
SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
```

### How KafkaSpout stores offsets of a Kafka topic and recovers in case of failures

As shown in the above KafkaConfig properties, you can control from where in the Kafka topic the 
spout begins to read by setting `KafkaConfig.startOffsetTime` as follows:

1. `kafka.api.OffsetRequest.EarliestTime()`:  read from the beginning of the topic 
(i.e. from the oldest messages onwards)
2. `kafka.api.OffsetRequest.LatestTime()`: read from the end of the topic
(i.e. any new messsages that are being written to the topic)
3. A Unix timestamp aka seconds since the epoch (e.g. via `System.currentTimeMillis()`):
   see [How do I accurately get offsets of messages for a certain timestamp using OffsetRequest?](https://cwiki.apache.org/confluence/display/KAFKA/FAQ#FAQ-HowdoIaccuratelygetoffsetsofmessagesforacertaintimestampusingOffsetRequest?) in the Kafka FAQ

As the topology runs the Kafka spout keeps track of the offsets it has read and emitted by storing 
state information under the ZooKeeper path `SpoutConfig.zkRoot+ "/" + SpoutConfig.id`.  In the case 
of failures it recovers from the last written offset in ZooKeeper.

> **Important:**  When re-deploying a topology make sure that the settings for `SpoutConfig.zkRoot`
and `SpoutConfig.id`
> were not modified, otherwise the spout will not be able to read its previous consumer state
information (i.e. the
> offsets) from ZooKeeper -- which may lead to unexpected behavior and/or to data loss, depending 
on your use case.

This means that when a topology has run once the setting `KafkaConfig.startOffsetTime` will not have
an effect for subsequent runs of the topology because now the topology will rely on the consumer 
state information (offsets) in ZooKeeper to determine from where it should begin 
(more precisely: resume) reading. If you want to force the spout to ignore any consumer state 
information stored in ZooKeeper, then you should set the parameter `KafkaConfig.ignoreZkOffsets` 
to `true`.  If `true`, the spout will always begin reading from the offset defined by 
`KafkaConfig.startOffsetTime` as described above.


## Using storm-kafka with different versions of kafka

Storm-kafka's Kafka dependency is defined as `provided` scope in maven, meaning it will not be 
pulled in as a transitive dependency. This allows you to use a version of Kafka dependency 
compatible with your kafka cluster.

When building a project with storm-kafka, you must explicitly add the Kafka dependency. For example,
to use Kafka 0.8.1.1 built against Scala 2.10, you would use the following dependency in your
`pom.xml`:

```xml
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.10</artifactId>
            <version>0.8.1.1</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.zookeeper</groupId>
                    <artifactId>zookeeper</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

Note that the ZooKeeper and log4j dependencies are excluded to prevent version conflicts with 
Storm's dependencies.

You can also override the kafka dependency version while building from maven, with parameter
`kafka.version` and `kafka.artifact.id` e.g. 
`mvn clean install -Dkafka.artifact.id=kafka_2.11 -Dkafka.version=0.9.0.1`

When selecting a kafka dependency version, you should ensure - 
 1. kafka api is compatible with storm-kafka. Currently, only 0.9.x and 0.8.x client API is 
 supported by storm-kafka module. If you want to use a higher version, storm-kafka-client module 
 should be used instead.
 2. The kafka client selected by you should be wire compatible with the broker. e.g. 0.9.x client 
 will not work with 0.8.x broker.

## Committer Sponsors

 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
