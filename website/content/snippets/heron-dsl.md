Heron processing topologies can be written using a domain-specific language called the **Heron DSL**. The Heron DSL is currently available for the following languages:

* [Java](../../../developers/java/dsl)
<!-- * [Python](../../../developers/python/dsl) -->

## The Heron DSL vs. the topology API

When Heron was first released, all Java topologies needed to be written using an API based on the [Storm topology API](../topologies). Although this API is quite powerful (and can still be used), the **Heron DSL** enables you to create topologies.

> A good way to think about the Heron DSL is to compare DSL-based topologies to [Apache Spark]() jobs.

There are some crucial differences between the two APIs:

Domain | Original topology API | Heron DSL
:------|:----------------------|:---------
Abstraction level | Low (developers must think in terms of "physical" spout and bolt implementation logic) | High ()
Processing model | [Spout](../spouts) and [bolt](../bolts) logic must be created explicitly, and connecting spouts and bolts is the responsibility of the developer | Spouts and bolts are created for you automatically on the basis of the processing graph that you build