---
title: Self-regulating topologies
---

As of version **TODO**, Heron provides support for **self-regulating topologies** via [Dhalion](#dhalion). Self-regulating topologies have numerous [benefits](#what-are-self-regulating-topologies) for both topology developers and Heron operators.

## Dhalion

Dhalion is an open source project spearheaded by Microsoft that seeks to add self-regulation and auto-tuning capabilities to distributed software systems. Heron is the first system to offer the benefits of Dhalion, although Dhalion could theoretically be added to other systems and may be in the future.

> For an in-depth look at Dhalion, check out the [Dhalion: Self-Regulating Stream Processing in Heron](https://www.microsoft.com/en-us/research/publication/dhalion-self-regulating-stream-processing-heron/) paper, co-authored by Heron co-creator [Karthik Ramasamy](https://twitter.com/karthikz) and long-time Heron contributor [Bill Graham](https://twitter.com/billgraham), along with [Avrilia Floratou](https://twitter.com/avrilia_here), [Ashvin Agrawal](https://www.linkedin.com/in/ashvinagrawal/), and [Sriram Rao](https://www.microsoft.com/en-us/research/people/sriramra/) from Microsoft.

Fundamentally, Dhalion works by periodically invoking [**policies**](#dhalion-policies) that evaluate the status of the topology and take appropriate action in case problems are identified. A topology developer may, for example, choose a Dhalion policy that maximizes topology throughput within specified resource constraints (without needing to over-provision topology resources).

## What are self-regulating topologies?

Self-regulating topologies are topologies that are:

* **Self tuning**, requiring no explicit specification of (a) topology-level resources like RAM, CPU, or containers or (b) operator-level resources like the number of partitioning.
* **Self stabilizing**, requiring no manual effort to adjust settings of running topologies to respond to events like extreme spikes in load
* **Self healing**, capable of identifying and automatically responding to service degradation due to things like malfunctioning or under-performing hardware

### Self tuning

A self-tuning topology is one that doesn't require you to explicitly configure a topology's resource consumption at *any* point in the topology's lifecycle. With a self-tuning topology, you can simply write your topology's processing logic---using either the [Functional API](../../concepts/topologies#the-heron-functional-api) (recommended) or the older Topology API---and set a few configs.

### Self stabilizing

A self-stabilizing topology is one that can handle unforeseen---and unforeseeable---events like spikes in processing load

Self-stabilizing have the additional advantage that they free you of the need to over-provision resources to handle potential spikes in load.

### Self healing

A self-healing topology is one that can automatically respond to problems like full-on hardware failure or degraded hardware performance.

### Objectives vs. resources

Self-regulating topologies take an **objective-driven** approach in which you specify objectives rather than resources for a topology. Once you specify those objectives,

Service-level objectives (SLOs)

## The drawbacks behind non-self-regulating topologies

Heron provides a wide variety of performance tuning "knobs" for topologies, enabling you as a topology developer to specify:

* how much RAM and CPU you'd like your topologies to use
* how many containers across which you'd like the topology's processing [instances](../../concepts/architecture#heron-instance) to be divided
* how many [partitions](TODO) you'd like each processing step to use

This affords fine-grained control over resource allocation, which is essential to using topologies in a resource-efficient way. But requiring you to explicitly configure these aspects of topologies has several drawbacks:

* It requires a trial-and-error approach to determine the appropriate RAM, CPU, etc. to assign to a topology. This type of "best effort" approach, even when undertaken with due care, consumes a lot of engineering resources and can be quite error prone, potentially leading to unforeseen problems in production environments.
* If a running topology encounters problems even in spite of your best efforts---excessive backpressure, processing failures, running out of resources---then you need to manually update and restart the topology. You may need to do this many times

## Dhalion policies

1. Dynamic resource provisioning for throughput maximization (self-stabilizing and self-healing)
2. Topology auto-tuning for meeting throughput SLOs (self-tuning)