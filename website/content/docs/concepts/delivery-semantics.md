---
title: Heron Delivery Semantics
---

Heron provides support for multiple delivery semantics.

Category | Description | When to use?
:--------|:------------|:------------
At most once | Heron processes tuples using a best-effort strategy. It's possible that some of the data injected into the system may be lost due to some combination of processing, machine, and network failures. What sets at-most-once semantics apart from the others is that Heron will not attempt a retry upon failure, which means that the tuple may fail to be delivered. | When some amount of data loss is acceptable
At least once | Tuples injected into the Heron topology are guaranteed to be processed at least once; no tuple will fail to be processed. It's possible, however, that any given tuple is processed more than once in the presence of various failures, retries, or other contingencies. | When you need to guarantee no data loss
Effectively once | Heron ensures that the data it receives is processed effectively once---even in the presence of various failures---leading to accurate results. **This applies only to ][stateful topologies](#stateful-processing)**. "Effectively" in this case means that there's a guarantee that tuples that cause [state changes](#stateful-processing) will be processed once (that is, they will have *an effect* on state once). | When you're using a [stateful topology](#stateful-processing) 

## Applying different delivery semantics to topologies

## Stateful processing

## Exactly-once semantics?

[This article](http://bravenewgeek.com/you-cannot-have-exactly-once-delivery/), for example, persuasively argues that it may be impossible. We use the term "effectively once" for Heron

Within a given topology, 