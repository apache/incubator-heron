---
title: Python DSL
description: A more functional approach to Heron stream processing
---

Prior to Heron version 0.15.0, writing stream processing logic for Heron in Python involved using the [topology API](../topologies), which requires you to manually stitch together data-producing [spouts](../spouts) and data-processing [bolts](../bolts) into a coherent processing model.

As of version 0.15.0, however, you can also use a specialized DSL (**d**omain-**s**pecific **l**anguage) that enables you to write processing logic in a style reminiscent of functional programming. With the Python DSL, you don't have to create spouts and bolts directly. Instead, you can use functional 

The core concept driving the Python DSL is that of the *streamlet*. Streamlets are indefinitely long sets of tuples that supply data to your stream processing logic (for example by pulling in data from a pub-sub system like [Kafka](https://kafka.apache.org/) or [Pulsar](http://pulsar.incubator.apache.org/)).

## Python DSL example

To begin, here's an example of a word count processor implemented using the DSL:

```python
topology_name = "my-word-count-topology"

counts = FixedLinesStreamlet.fixedLinesGenerator(parallelism=2) \
         .flat_map(lambda line: line.split(), parallelism=2) \
         .map(lambda word: (word, 1), parallelism=2)

counts.run(topology_name)
```

Here, the [`FixedLinesStreamlet`]({{% githubMaster %}}/heronpy/connectors/mock/fixedlinesstreamlet.py) initiates the processing graph by supplying an indefinite series of sentences chosen from a static list (sentences like "Humpy Dumpty sat on a wall"). From there:

* the `flat_map` operation splits the line into separate words and returns a list of words
* the `map` operation transforms each word into a tuple in which the first element is the word and the second element is the integer 1

## The DSL vs. the topology API

In this example, `counts` is *technically* a topology but it isn't specified [like a normal topology](../topologies). Instead of spouts, a streamlet Instead of [bolts](../bolts), a series of functions is used to process incoming data. In the Python DSL, these processing functions essentially do one thing: they take a streamlet and transform it into a new streamlet. You can apply as many streamlet-transforming functions as you like, and end the chain whenever you've achieved your desired result set.

A few other things to notice:

* Each step in the graph, including the original streamlet, has a defined `parallelism` attribute. This determines the number of processes that will be spawned in the Heron cluster to handle that transformation. Supplying a `parallelism` attribute is optional; if not supplied, a parallelism hint of 1 will be applied.
* The processing steps specified by the graph will not be initiated until the `run` function is called. The name of the topology needs to be passed to this function.

## Processing functions

The Python DSL for Heron exposes a variety of functions that you can use to transform streamlets (or rather, transform streamlets into other streamlets):

Function name | Description
:-------------|:------------
[`map`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.Streamlet.map) | Returns a new streamlet by applying the supplied mapping function to each element in the original streamlet
[`flat_map`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.Streamlet.flat_map) | Like `map` but with the important difference that each element of the streamlet is flattened
[`join`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.Streamlet.join) | Enables you to join two separate streamlets into a single streamlet
[`filter`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.Streamlet.filter) | Returns a new streamlet containing only the elements that satisfy the supplied filtering function
[`sample`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.Streamlet.sample) | Returns a new streamlet containing only a fraction of elements. That fraction is defined by the supplied function.
[`repartition`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.Streamlet.repartition) | Returns a new streamlet with a new parallelism level
[`reduce_by_key_and_window`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.Streamlet.reduce_by_key_and_window) | Returns a new streamlet in which each key-value pair of this streamlet is collected over a specified time window and reduced using a specified reduce function. More information on time windowing can be found [below](#windowing).
[`reduce_by_window`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.Streamlet.reduce_by_window) | A shortcut for `reduce_by_key_and_window` with a parallelism of 1 over the specified time window and reduced using a specified reduce function. More information on time windowing can be found [below](#windowing).

## Creating streamlets

Currently, creating input streamlets using the Python DSL for Heron involves wrapping a [`Spout`](/api/python/api/spout/spout.m.html) object. You can see an example below:

```python
import random

from heronpy.dsl.src.python import OperationType, Streamlet
from heronpy.api.src.python import Spout

class RandomFruitSpout(Spout):
    def initialize(self, config, context):
        self.words = ["apple", "orange", "banana", "lime", "tangelo"]

    def next_tuple(self):
        self.emit(random.choice(self.words))

class RandomFruitStreamlet(Streamlet):
    def __init__(self, stage_name=None, parallelism=None):
        super(RandomFruitStreamlet, self).__init__(
            parents=[],
            operation=OperationType.Input,
            stage_name=stage_name,
            parallelism=parallelism)
    
    @staticmethod
    def random_fruit_streamlet(stage_name=None, parallelism=None):
        return RandomFruitStreamlet(stage_name, parallelism)
    
    def _build_this(self, builder):
        builder.add_spout(
            self._stage_name,
            RandomFruitSpout,
            par=self._parallelism)
```

In this example, the `RandomFruitSpout` implements the [`Spout`](/api/python/api/spout/spout.m.html) interface and emits names of fruit from a pre-selected list (apple, orange, etc.). The `RandomFruitStreamlet` implements the [`Streamlet`](/api/python/dsl/streamlet.m.html) interface. The `__init__` function defines how new `RandomFruitStreamlet`s are instantiated, while the `_build_this` function

Here's a simple processing graph created using this input streamlet:

```python
def process_fruit(fruit):
    print("The fruit {} was selected".format(fruit))

fruits_graph = RandomFruitStreamlet.random_fruit_streamlet(stage_name='input', parallelism=3)
               .map(lambda fruit: process_fruit(fruit))
```

## Windowing

Windowing is the process of gathering tuples over a specified duration of time, creating a new streamlet by applying some kind of reduce logic to the gathered tuples, and returning the resulting streamlet at the end of the period. An example would be returning a count of words in a word count topology every minute.

In general, windowing operations in the Python DSL require specifying a [`TimeWindow`](/api/python/dsl/streamlet.m.html#heronpy.dsl.streamlet.TimeWindow) object that specifies the window itself.

```python
from heronpy.dsl.streamlet import TimeWindow

counts = FixedLinesStreamlet.fixedLinesGenerator(parallelism=2) \
         .flat_map(lambda line: line.split(), parallelism=2) \
         .map(lambda word: (word, 1), parallelism=2)
         .reduce_by_window()
```

## Setup

To create Heron topologies using the Python DSL, you first need to install the `heronpy` library using [pip](https://pip.pypa.io/en/stable/), [EasyInstall](https://wiki.python.org/moin/EasyInstall), or an analogous tool:

```shell
$ pip install heronpy
$ easy_install heronpy
```

Then you can include `heronpy` in your project files. Here's an example:

```python
from heronpy.dsl.streamlet import Streamlet 
```