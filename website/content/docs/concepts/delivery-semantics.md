---
title: Heron Delivery Semantics
---

Heron provides support for multiple delivery semantics.

Category | Description | When to use?
:--------|:------------|:------------
At most once | Heron processes tuples using a best-effort strategy. It's possible that some of the data injected into the system may be lost due to some combination of processing, machine, and network failures. What sets at-most-once semantics apart from the others is that Heron will not attempt a retry upon failure, which means that the tuple may fail to be delivered. | When some amount of data loss is acceptable
At least once | Tuples injected into the Heron topology are guaranteed to be processed at least once; no tuple will fail to be processed. It's possible, however, that any given tuple is processed more than once in the presence of various failures, retries, or other contingencies. | When you need to guarantee no data loss
Effectively once | Heron ensures that the data it receives is processed effectively once---even in the presence of various failures---leading to accurate results. **This applies only to ][stateful topologies](#stateful-processing)**. "Effectively" in this case means that there's a guarantee that tuples that cause [state changes](#stateful-processing) will be processed once (that is, they will have *an effect* on state once). | When you're using a [stateful topology](#stateful-processing)

![Heron delivery semantics](/img/delivery-semantics.svg)

## Applying different delivery semantics to topologies

In Heron, you can choose delivery semantics on a topology-by-topology basis. If you have topologies for which at-most-once semantics are perfectly acceptable, you can run them alongside topologies with more stringent semantics.

## Stateful processing

The Heron topologies that you create 

Effectively once processing 

## Exactly-once semantics?

There has been a lot of discussion recently surrounding so-called "exactly-once" processing semantics. We'll avoid this term

Instead, we'll use the term **effectively once** here, following [Victor Klang](https://twitter.com/viktorklang/status/789036133434978304)

[This article](http://bravenewgeek.com/you-cannot-have-exactly-once-delivery/), for example, persuasively argues that it may be impossible. We use the term "effectively once" for Heron

Within a given topology, 