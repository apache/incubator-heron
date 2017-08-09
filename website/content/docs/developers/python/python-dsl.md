---
title: Python DSL
description: A more functional approach to Heron stream processing
---

Prior to Heron version 0.15.0, writing stream processing logic for Heron in Python involved using the [topology API](../topologies), which requires you to manually stitch together data-producing [spouts](../spouts) and data-processing [bolts](../bolts) into a coherent processing model.

As of version 0.15.0, however, you can also use a specialized DSL (domain-specific language) that enables you to write processing logic in a style reminiscent of functional programming. With the Python DSL, you don't have to create spouts and bolts directly. Instead, you can use functional 

The core concept driving the Python DSL is that of the *streamlet*. Streamlets are indefinitely long sets of tuples that supply data to your stream processing logic (perhaps by pulling in data from a pub-sub system like [Kafka](https://kafka.apache.org/) or [Pulsar](http://pulsar.incubator.apache.org/)).

## Python DSL example

To begin, here's an example of a word count processor implemented using the DSL:

```python
topology_name = "my-word-count-topology"

counts = FixedLinesStreamlet.fixedLinesGenerator(parallelism=2) \
         .flatMap(lambda line: line.split(), parallelism=2) \
         .map(lambda word: (word, 1), parallelism=2)

counts.run(topology_name)
```

Here, the `FixedLinesStreamlet` produces a never-ending series

In this example, `counts` is technically a topology but it isn't specified [like a normal topology](../topologies). Instead of [bolts](../bolts), a series of functions is used to process incoming data. In the Python DSL, these processing functions take a streamlet and transform it into a new streamlet. You can apply as many streamlet-transforming functions as you like, and end the chain whenever you've achieved your desired result set.

A few other things to notice:

* Each step in the graph, including the original streamlet, has a defined `parallelism` attribute. This determines the number of processes that will be spawned in the Heron cluster to handle that transformation.
* The processing steps specified by the graph will not be initiated until the `run` function is called. The name of the topology needs to be passed to this function.

## Processing functions

Function name | Description
:-------------|:------------
`map` | Returns a new streamlet by applying the supplied mapping function to each element in the original streamlet
`flatMap` | Like `map` but with the important difference that each element of the streamlet is flattened
`join` | Enables you to join two separate streamlets into a single streamlet
`filter` | Returns a new streamlet containing only the elements that satisfy the supplied filtering function
`sample` | Returns a new streamlet containing only a fraction of elements. That fraction is defined by the supplied function.
`repartition` | Returns a new streamlet with a new parallelism level
`reduceByWindow` |

## Creating streamlets

```python

```

## Windowing