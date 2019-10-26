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
