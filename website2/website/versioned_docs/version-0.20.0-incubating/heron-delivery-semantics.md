---
id: version-0.20.0-incubating-heron-delivery-semantics
title: Heron Delivery Semantics
sidebar_label: Heron Delivery Semantics
original_id: heron-delivery-semantics
---
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

Heron provides support for multiple delivery semantics, and you can select delivery semantics on a topology-by-topology basis. Thus, if you have topologies for which [at-most-once](#available-semantics) semantics are perfectly acceptable, for example, you can run them alongside topologies with more stringent semantics (such as effectively once).

## Available semantics

Heron supports three delivery semantics:

Semantics | Description | When to use?
:---------|:------------|:------------
At most once | Heron processes tuples using a best-effort strategy. With at-most-once semantics, it's possible that some of the tuples delivered into the system may be lost due to some combination of processing, machine, and network failures. What sets at-most-once semantics apart from the others is that Heron will not attempt to retry a processing step upon failure, which means that the tuple may fail to be delivered. | When some amount of data loss is acceptable
At least once | Tuples injected into the Heron topology are guaranteed to be processed at least once; no tuple will fail to be processed. It's possible, however, that any given tuple is processed more than once in the presence of various failures, retries, or other contingencies. | When you need to guarantee no data loss
Effectively once | Heron ensures that the data it receives is processed effectively once---even in the presence of various failures---leading to accurate results. **This applies only to [stateful topologies](#stateful-topologies)**. "Effectively" in this case means that there's a guarantee that tuples that cause [state changes](#stateful-topologies) will be processed once (that is, they will have *an effect* on state once). | When you're using [stateful topologies](#stateful-processing) and need strong 

You can see a visual representation of these different delivery semantics in the figure below:

![Heron delivery semantics](https://www.lucidchart.com/publicSegments/view/f35df5fd-bfc1-4270-aad6-40766abae024/image.png)

In this diagram, you see three Heron topologies, each of which is processing a series of tuples (`(1,2,3)`, `(7,8,11)`, etc.).

* The topology in the upper left offers at-most-once semantics, which means that each tuple will either be delivered once or fail to be processed. In this case, the `(1,5)` tuple fails to be processed.
* The topology in the lower left offers at-least-once semantics, which means that each tuple will be delivered either once or more than once. In this case, the `(7,8,11)` tuple is processed more than once (perhaps due to a network glitch or a retry).
* The topology in the upper right offers effectively once semantics, which means that every tuple is delivered one time and one time only. This does *not* mean that every tuple is processed exactly one time. Some tuples may be processed multiple times *within the topology*, but we use the "effectively once" terminology here to express that

## Requirements for effectively once

In order to use effectively-once semantics with a topology, that topology must satisfy two conditions:

1. It must be a [stateful, idempotent topology](#stateful-topologies).
2. The input stream into the topology must be strongly consistent. In order to provide effectively-once semantics, topologies need to be able to "rewind" state in case of failure. The state that it "rewinds" needs to be reliable state---preferably durably stored.

    If the input to the topology is, for example, a messaging system that cannot ensure stream consistency, then effectively-once semantics cannot be applied, as the state "rewind" may return differing results. To put it somewhat differently, Heron can only provide delivery semantics as stringent as its data input sources can themselves provide.

### Exactly-once semantics?

There has been a lot of discussion recently surrounding so-called "exactly-once" processing semantics. We'll avoid this term in the Heron documentation because we feel that it's misleading. "Exactly-once" semantics would mean that no processing step is ever performed more than once---and thus that no processing step is ever retried.

It's important to always keep in mind that *no system* can provide exactly-once semantics in the face of failures (as [this article](http://bravenewgeek.com/you-cannot-have-exactly-once-delivery) argues). But that's okay because they don't really need to; the truly important thing is that a stream processing system be able to recover from failures by "rewinding" state to a previous, pre-failure point and to re-attempt to apply processing logic. We use the tern **effectively once**, following [Victor Klang](https://twitter.com/viktorklang/status/789036133434978304), for this style of semantics.

Heron *can* provide effectively-once guarantees if a topology meets the conditions [outlined above](#requirements-for-effectively-once), but it cannot provide "exactly-once" semantics.

## Stateful topologies

The Heron topologies that you create can be either stateful or non stateful.

* In **stateful topologies**, each component must implement an interface that requires it to store its state every time it processes a tuple (both spouts *and* bolts must do so).
* In **non-stateful topologies**, there is no requirement that any processing components store a state snapshot. Non-stateful topologies can provide at-most-once or at-least-once semantics, but never effectively-once semantics.

> Heron currently supports two state managers: [ZooKeeper](state-managers-zookeeper) and the [local filesystem](state-managers-local-fs), although others are currently under development.

Stateful topologies, in turn, are of two types:

* **Idempotent** stateful topologies are stateful topologies in which applying the processing graph to an input more than once, it will continue to return the same result. A basic example is multiplying a number by 0. The first time you do so, the number will change (always to 0), but if you apply that transformation again and again, it will not change.

    For topologies to provide effectively-once semantics, they need to transform tuple inputs idempotently as well. If they don't, and applying the topology's processing graph multiple times yields different results, then effectively-once semantics *cannot* be achieved.

    If you'd like to create idempotent stateful topologies, make sure to write tests to ensure that idempotency requirements are being met.
* **Non-idempotent** stateful topologies are stateful topologies that do not apply processing logic along the model of "multiply by zero" and thus cannot provide effectively-once semantics. An example of a non-idempotent

> Remember: effectively-once semantics can only be applied to topologies that are: (a) stateful and (b) idempotent.