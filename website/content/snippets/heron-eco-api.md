Heron processing topologies can be written using an API called the **Heron ECO API**. The Heron ECO API is currently available for the following languages:

* [Java](../../../developers/java/eco-api)

> Although this document focuses on the ECO API, both the [Streamlet API](../../../concepts/streamlet-api) and [topology API](../../../concepts/topologies) topologies you have built will can stil be used with Heron

## The Heron ECO API vs. The Streamlet and Topology APIs

Heron's ECO offers one major difference over the Streamlet and Topology APIs and that is extensibility without recompilation.
With Heron's ECO developers now have a way to alter the way data flows through spouts and bolts without needing to get into their code and make changes.
Topologies can now be defined through a YAML based format.

## What about Storm Flux?  Is it compatible with  Eco?

We built ECO with Flux in mind.  Most Storm Flux topologies should be able to deployed in Heron with minimal changes.
Start reading  [Migrate Storm To Heron] (../../docs/migrate-storm-to-heron) to learn how to migrate your Storm Flux topology then come back.