---
title: Self-regulating topologies
---

As of version **TODO**, Heron provides support for self-regulating topologies via [Dhalion](#dhalion).

## Dhalion

[Dhalion](https://www.microsoft.com/en-us/research/publication/dhalion-self-regulating-stream-processing-heron/) is an open source project spearheaded by Microsoft.

## What are self-regulating topologies?

### Self tuning

A topology that is self tuning is one that doesn't require you to explicitly configure a topology's resource consumption.

### Self stabilizing

### Self healing

### Objectives vs. resources

Self-regulating topologies take an **objective-driven** approach in which you specify objectives rather than resources for a topology. Once you specify those objectives,

## The drawbacks behind non-self-regulating topologies

Heron provides a wide variety of performance tuning "knobs" for topologies, enabling you as a topology developer to specify:

* how much RAM and CPU you'd like your topologies to use
* how many containers across which you'd like the topology's processing [instances](../../concepts/architecture#heron-instance) to be divided
* how many [partitions](TODO) you'd like each processing step to use

This affords fine-grained control over resource allocation, which is essential to using topologies in a resource-efficient way. But requiring you to explicitly configure these aspects of topologies has several drawbacks:

* It requires a trial-and-error approach to determine the appropriate RAM, CPU, etc. to assign to a topology. This type of "best effort" approach, even when undertaken with due care, consumes a lot of engineering resources and can be quite error prone, potentially leading to unforeseen problems in production environments.
* If a running topology encounters problems even in spite of your best efforts---excessive backpressure, processing failures, running out of resources---then you need to manually update and restart the topology. You may need to do this many times

## The benefit of self-regulating topologies