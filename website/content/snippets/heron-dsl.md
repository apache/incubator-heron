Heron processing topologies can be written using a domain-specific language called the **Heron DSL**. The Heron DSL is currently available for the following languages:

* [Java](../../../developers/java/dsl)
<!-- * [Python](../../../developers/python/dsl) -->

> Although the original [topology API](../../../concepts/topologies) can still be used with Heron (which means that all of your older topologies will still run) we strongly recommend creating all new topologies using the Heron DSL, for reasons outlined in the section below.

## The Heron DSL vs. the topology API

When Heron was first released, all Java topologies needed to be written using an API based on the [Storm topology API](../topologies). Although this API is quite powerful (and can still be used), the **Heron DSL** enables you to create topologies.

There are some crucial differences between the two APIs:

Domain | Original topology API | Heron DSL
:------|:----------------------|:---------
Programming style | Procedural | Declarative, functional
Abstraction | **Low level**. Developers must think in terms of "physical" spout and bolt implementation logic) | **High level**. Developers can write processing logic in an idiomatic fashion in the language of their choice, without needing to write and connect spouts and bolts.
Processing model | [Spout](../spouts) and [bolt](../bolts) logic must be created explicitly, and connecting spouts and bolts is the responsibility of the developer | Spouts and bolts are created for you automatically on the basis of the processing graph that you build

The two APIs also have a few things in common:

* Containerization of topology components is handled automatically by Heron
* Topologies are [managed](../../operators/heron-shell) in the same way