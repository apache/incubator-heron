<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
---
title: Implementing a Spout
---

{{< alert "spouts-and-bolts" >}}

Spouts must implement the [`ISpout`](/api/org/apache/heron/api/spout/ISpout.html) interface.

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
to [`OutputCollector`](/api/org/apache/heron/api/bolt/).

* The `ack` method is called when the `Tuple` with the `msgId` emitted by this
spout is successfully processed.

* The `fail` method is called when the `Tuple` with the `msgId` emitted by this
spout is not processed successfully.

See [`TestWordSpout`](https://github.com/apache/incubator-heron/blob/master/examples/src/java/org/apache/heron/examples/api/spout/TestWordSpout.java) for a simple spout example.

Instead of implementing the [`ISpout`](/api/org/apache/heron/api/spout/ISpout.html) interface directly, you can also implement [`IRichSpout`](/api/org/apache/heron/api/spout/IRichSpout.html).
