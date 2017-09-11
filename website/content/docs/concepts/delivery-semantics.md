---
title: Heron Delivery Semantics
---

Heron provides support for multiple delivery semantics.

Category | Description
:--------|:-----------
At most once | Heron processes tuples using a best-effort strategy. It's possible that some of the data injected into the system may be lost due to some combination of processing, machine, and network failures.
At least once | Tuples injected into the Heron topology are guaranteed to be processed at least once; no tuple will fail to be processed. It's possible, however, that any given tuple is processed more than once in the presence of various failures, retries, or other contingencies.
Effectively once | Heron ensures that the data it receives is processed effectively once---even in the presence of various failures---leading to accurate results. This applies only to stateful topologies. "Effectively" here means that

## Exactly-once semantics?

[This article](http://bravenewgeek.com/you-cannot-have-exactly-once-delivery/), for example, persuasively argues that it may be impossible. We use the term "effectively once" for Heron

Within a given topology, 