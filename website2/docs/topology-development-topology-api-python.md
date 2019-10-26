---
id: topology-development-topology-api-python
title: The Heron Topology API for Python
sidebar_label: The Heron Topology API for Python
---
<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

> The current version of `heronpy` is [{{heron:version}}](https://pypi.python.org/pypi/heronpy/{{heron:version}}).

Support for developing Heron topologies in Python is provided by a Python library called [`heronpy`](https://pypi.python.org/pypi/heronpy).

> #### Python API docs
> You can find API docs for the `heronpy` library [here](/api/python).

## Setup

First, you need to install the `heronpy` library using [pip](https://pip.pypa.io/en/stable/), [EasyInstall](https://wiki.python.org/moin/EasyInstall), or an analogous tool:

```shell
$ pip install heronpy
$ easy_install heronpy
```

Then you can include `heronpy` in your project files. Here's an example:

```python
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.spout.spout import Spout
from heronpy.api.topology import Topology
```

## Writing topologies in Python

Heron [topologies](heron-topology-concepts) are networks of [spouts](heron-topology-concepts#spouts) that pull data into a topology and [bolts](heron-topology-concepts#bolts) that process that ingested data.

> You can see how to create Python spouts in the [Implementing Python Spouts](#spouts) guide and how to create Python bolts in the [Implementing Python Bolts](#bolts) guide.

Once you've defined spouts and bolts for a topology, you can then compose the topology in one of two ways:

* You can use the [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder) class inside of a main function.

    Here's an example:

    ```python
    #!/usr/bin/env python
    from heronpy.api.topology import TopologyBuilder


    if __name__ == "__main__":
        builder = TopologyBuilder("MyTopology")
        # Add spouts and bolts
        builder.build_and_submit()
    ```

* You can subclass the [`Topology`](/api/python/topology.m.html#heronpy.topology.Topology) class.

    Here's an example:

    ```python
    from heronpy.api.stream import Grouping
    from heronpy.api.topology import Topology


    class MyTopology(Topology):
        my_spout = WordSpout.spec(par=2)
        my_bolt = CountBolt.spec(par=3, inputs={spout: Grouping.fields("word")})
    ```

## Defining topologies using the [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder) class

If you create a Python topology using a [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder), you need to instantiate a `TopologyBuilder` inside of a standard Python main function, like this:

```python
from heronpy.api.topology import TopologyBuilder


if __name__ == "__main__":
    builder = TopologyBuilder("MyTopology")
```

Once you've created a `TopologyBuilder` object, you can add [bolts](#bolts) using the [`add_bolt`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder.add_bolt) method and [spouts](#spouts) using the [`add_spout`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder.add_spout) method. Here's an example:

```python
builder = TopologyBuilder("MyTopology")
builder.add_bolt("my_bolt", CountBolt, par=3)
builder.add_spout("my_spout", WordSpout, par=2)
```

Both the `add_bolt` and `add_spout` methods return the corresponding [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec) object.

The `add_bolt` method takes four arguments and an optional `config` parameter:

Argument | Data type | Description | Default
:--------|:----------|:------------|:-------
`name` | `str` | The unique identifier assigned to this bolt | |
`bolt_cls` | class | The subclass of [`Bolt`](/api/python/bolt/bolt.m.html#heronpy.bolt.bolt.Bolt) that defines this bolt | |
`par` | `int` | The number of instances of this bolt in the topology | |
`config` | `dict` | Specifies the configuration for this spout | `None`

The `add_spout` method takes three arguments and an optional `config` parameter:

Argument | Data type | Description | Default
:--------|:----------|:------------|:-------
`name` | `str` | The unique identifier assigned to this spout | |
`spout_cls` | class | The subclass of [`Spout`](/api/python/spout/spout.m.html#heronpy.spout.spout.Spout) that defines this spout | |
`par` | `int` | The number of instances of this spout in the topology | |
`inputs` | `dict` or `list` | Either a `dict` mapping from [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec) to [`Grouping`](/api/python/stream.m.html#heronpy.stream.Grouping) *or* a list of [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec)s, in which case the [`shuffle`](/api/python/stream.m.html#heronpy.stream.Grouping.SHUFFLE) grouping is used
`config` | `dict` | Specifies the configuration for this spout | `None`

### Example

The following is an example implementation of a word count topology in Python that subclasses [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder).

```python
from your_spout import WordSpout
from your_bolt import CountBolt

from heronpy.api.stream import Grouping
from heronpy.api.topology import TopologyBuilder


if __name__ == "__main__":
    builder = TopologyBuilder("WordCountTopology")
    # piece together the topology
    word_spout = builder.add_spout("word_spout", WordSpout, par=2)
    count_bolt = builder.add_bolt("count_bolt", CountBolt, par=2, inputs={word_spout: Grouping.fields("word")})
    # submit the toplogy
    builder.build_and_submit()
```

Note that arguments to the main method can be passed by providing them in the
`heron submit` command.

### Topology-wide configuration

If you're building a Python topology using a `TopologyBuilder`, you can specify configuration for the topology using the [`set_config`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder.set_config) method. A topology's config is a `dict` in which the keys are a series constants from the [`api_constants`](/api/python/api_constants.m.html) module and values are configuration values for those parameters.

Here's an example:

```python
from heronpy.api import api_constants
from heronpy.api.topology import TopologyBuilder


if __name__ == "__main__":
    topology_config = {
        api_constants.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS: True
    }
    builder = TopologyBuilder("MyTopology")
    builder.set_config(topology_config)
    # Add bolts and spouts, etc.
```

### Launching the topology

If you want to [submit](../../../operators/heron-cli#submitting-a-topology) Python topologies to a Heron cluster, they need to be packaged as a [PEX](https://pex.readthedocs.io/en/stable/whatispex.html) file. In order to produce PEX files, we recommend using a build tool like [Pants](http://www.pantsbuild.org/python_readme.html) or [Bazel](https://github.com/benley/bazel_rules_pex).

If you defined your topology by subclassing the [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder) class and built a `word_count.pex` file for that topology in the `~/topology` folder. You can submit the topology to a cluster called `local` like this:

```bash
$ heron submit local \
  ~/topology/word_count.pex \
  - # No class specified
```

Note the `-` in this submission command. If you define a topology by subclassing `TopologyBuilder` you do not need to instruct Heron where your main method is located.

> #### Example topologies buildable as PEXs
> * See [this repo](https://github.com/streamlio/pants-dev-environment) for an example of a Heron topology written in Python and deployable as a Pants-packaged PEX.
> * See [this repo](https://github.com/streamlio/bazel-dev-environment) for an example of a Heron topology written in Python and deployable as a Bazel-packaged PEX.

## Defining a topology by subclassing the [`Topology`](/api/python/topology.m.html#heronpy.topology.Topology) class

If you create a Python topology by subclassing the [`Topology`](/api/python/topology.m.html#heronpy.topology.Topology) class, you need to create a new topology class, like this:

```python
from my_spout import WordSpout
from my_bolt import CountBolt

from heronpy.api.stream import Grouping
from heronpy.api.topology import Topology


class MyTopology(Topology):
    my_spout = WordSpout.spec(par=2)
    my_bolt_inputs = {my_spout: Grouping.fields("word")}
    my_bolt = CountBolt.spec(par=3, inputs=my_bolt_inputs)
```

All you need to do is place [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec)s as the class attributes
of your topology class, which are returned by the `spec()` method of
your spout or bolt class. You do *not* need to run a `build` method or anything like that; the `Topology` class will automatically detect which spouts and bolts are included in the topology.

> If you use this method to define a new Python topology, you do *not* need to have a main function.

For bolts, the [`spec`](/api/python/bolt/bolt.m.html#heronpy.bolt.bolt.Bolt.spec) method for spouts takes three optional arguments::

Argument | Data type | Description | Default
:--------|:----------|:------------|:-------
`name` | `str` | The unique identifier assigned to this bolt or `None` if you want to use the variable name of the return `HeronComponentSpec` as the unique identifier for this bolt | |
`par` | `int` | The number of instances of this bolt in the topology | |
`config` | `dict` | Specifies the configuration for this bolt | `None`


For spouts, the [`spec`](/api/python/spout/spout.m.html#heronpy.spout.spout.Spout.spec) method takes four optional arguments:

Argument | Data type | Description | Default
:--------|:----------|:------------|:-------
`name` | `str` | The unique identifier assigned to this spout or `None` if you want to use the variable name of the return `HeronComponentSpec` as the unique identifier for this spout | `None` |
`inputs` | `dict` or `list` | Either a `dict` mapping from [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec) to [`Grouping`](/api/python/stream.m.html#heronpy.stream.Grouping) *or* a list of [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec)s, in which case the [`shuffle`](/api/python/stream.m.html#heronpy.stream.Grouping.SHUFFLE) grouping is used
`par` | `int` | The number of instances of this spout in the topology | `1` |
`config` | `dict` | Specifies the configuration for this spout | `None`

### Example

Here's an example topology definition with one spout and one bolt:

```python
from my_spout import WordSpout
from my_bolt import CountBolt

from heronpy.api.stream import Grouping
from heronpy.api.topology import Topology


class WordCount(Topology):
    word_spout = WordSpout.spec(par=2)
    count_bolt = CountBolt.spec(par=2, inputs={word_spout: Grouping.fields("word")})
```

### Launching

If you defined your topology by subclassing the [`Topology`](/api/python/topology.m.html#heronpy.topology.Topology) class,
your main Python file should *not* contain a main method. You will, however, need to instruct Heron which class contains your topology definition.

Let's say that you've defined a topology by subclassing `Topology` and built a PEX stored in `~/topology/dist/word_count.pex`. The class containing your topology definition is `topology.word_count.WordCount`. You can submit the topology to a cluster called `local` like this:

```bash
$ heron submit local \
  ~/topology/dist/word_count.pex \
  topology.word_count.WordCount \ # Specifies the topology class definition
  WordCountTopology
```

### Topology-wide configuration

If you're building a Python topology by subclassing `Topology`, you can specify configuration for the topology using the [`set_config`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder.set_config) method. A topology's config is a `dict` in which the keys are a series constants from the [`api_constants`](/api/python/api_constants.m.html) module and values are configuration values for those parameters.

Here's an example:

```python
from heronpy.api.topology import Topology
from heronpy.api import api_constants


class MyTopology(Topology):
    config = {
        api_constants.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS: True
    }
    # Add bolts and spouts, etc.
```

## Multiple streams

To specify that a component has multiple output streams, instead of using a list of
strings for `outputs`, you can specify a list of `Stream` objects, in the following manner.

```python
class MultiStreamSpout(Spout):
    outputs = [
        Stream(fields=["normal", "fields"], name="default"),
        Stream(fields=["error_message"], name="error_stream"),
    ]
```

To select one of these streams as the input for your bolt, you can simply
use `[]` to specify the stream you want. Without any stream specified, the `default`
stream will be used.

```python
class MultiStreamTopology(Topology):
    spout = MultiStreamSpout.spec()
    error_bolt = ErrorBolt.spec(inputs={spout["error_stream"]: Grouping.LOWEST})
    consume_bolt = ConsumeBolt.spec(inputs={spout: Grouping.SHUFFLE})
```

## Declaring output fields using the `spec()` method

In Python topologies, the output fields of your spouts and bolts
need to be declared by placing `outputs` class attributes, as there is
no `declareOutputFields()` method. `heronpy` enables you to dynamically declare output fields as a list using the
`optional_outputs` argument in the `spec()` method.

This is useful in a situation like below.

```python
class IdentityBolt(Bolt):
    # Statically declaring output fields is not allowed
    class process(self, tup):
        emit([tup.values])


class DynamicOutputField(Topology):
    spout = WordSpout.spec()
    bolt = IdentityBolt.spec(inputs={spout: Grouping.ALL}, optional_outputs=["word"])
```

You can also declare outputs in the `add_spout()` and the `add_bolt()`
method for the `TopologyBuilder` in the same way.

## Example topologies

There are a number of example topologies that you can peruse in the [`examples/src/python`]({{% githubMaster %}}/examples/src/python) directory of the [Heron repo]({{% githubMaster %}}):

Topology | File | Description
:--------|:-----|:-----------
Word count | [`word_count_topology.py`]({{% githubMaster %}}/examples/src/python/word_count_topology.py) | The [`WordSpout`]({{% githubMaster %}}/examples/src/python/spout/word_spout.py) spout emits random words from a list, while the [`CountBolt`]({{% githubMaster %}}/examples/src/python/bolt/count_bolt.py) bolt counts the number of words that have been emitted.
Multiple streams | [`multi_stream_topology.py`]({{% githubMaster %}}/examples/src/python/multi_stream_topology.py) | The [`MultiStreamSpout`]({{% githubMaster %}}/examples/src/python/spout/multi_stream_spout.py) emits multiple streams to downstream bolts.
Half acking | [`half_acking_topology.py`]({{% githubMaster %}}/examples/src/python/half_acking_topology.py) | The [`HalfAckBolt`]({{% githubMaster %}}/examples/src/python/bolt/half_ack_bolt.py) acks only half of all received tuples.
Custom grouping | [`custom_grouping_topology.py`]({{% githubMaster %}}/examples/src/python/custom_grouping_topology.py) | The [`SampleCustomGrouping`]({{% githubMaster %}}/examples/src/python/custom_grouping_topology.py#L26) class provides a custom field grouping.

You can build the respective PEXs for these topologies using the following commands:

```shell
$ bazel build examples/src/python:word_count
$ bazel build examples/src/python:multi_stream
$ bazel build examples/src/python:half_acking
$ bazel build examples/src/python:custom_grouping
```

All built PEXs will be stored in `bazel-bin/examples/src/python`. You can submit them to Heron like so:

```shell
$ heron submit local \
  bazel-bin/examples/src/python/word_count.pex - \
  WordCount
$ heron submit local \
  bazel-bin/examples/src/python/multi_stream.pex \
  heron.examples.src.python.multi_stream_topology.MultiStream
$ heron submit local \
  bazel-bin/examples/src/python/half_acking.pex - \
  HalfAcking
$ heron submit local \
  bazel-bin/examples/src/python/custom_grouping.pex \
  heron.examples.src.python.custom_grouping_topology.CustomGrouping
```

By default, the `submit` command also activates topologies. To disable this behavior, set the `--deploy-deactivated` flag.

## Bolts 

 Bolts must implement the `Bolt` interface, which has the following methods.

```python
class MyBolt(Bolt):
    def initialize(self, config, context): pass
    def process(self, tup): pass
```

* The `initialize()` method is called when the bolt is first initialized and
provides the bolt with the executing environment. It is equivalent to `prepare()`
method of the [`IBolt`](/api/org/apache/heron/api/bolt/IBolt.html) interface in Java.
Note that you should not override `__init__()` constructor of `Bolt` class
for initialization of custom variables, since it is used internally by HeronInstance; instead,
`initialize()` should be used to initialize any custom variables or connections to databases.

* The `process()` method is called to process a single input `tup` of `HeronTuple` type. This method
is equivalent to `execute()` method of `IBolt` interface in Java. You can use
`self.emit()` method to emit the result, as described below.

In addition, `BaseBolt` class provides you with the following methods.

```python
class BaseBolt(BaseComponent):
    def emit(self, tup, stream="default", anchors=None, direct_task=None, need_task_ids=False): ...
    def ack(self, tup): ...
    def fail(self, tup): ...
    def log(self, message, level=None): ...
    @staticmethod
    def is_tick(tup)
    @classmethod
    def spec(cls, name=None, inputs=None, par=1, config=None): ...
```

* The `emit()` method is used to emit a given `tup`, which can be a `list` or `tuple` of
any python objects. Unlike the Java implementation, `OutputCollector`
doesn't exist in the Python implementation.

* The `ack()` method is used to indicate that processing of a tuple has succeeded.

* The `fail()` method is used to indicate that processing of a tuple has failed.

* The `is_tick()` method returns whether a given `tup` of `HeronTuple` type is a tick tuple.

* The `log()` method is used to log an arbitrary message, and its outputs are redirected
  to the log file of the component. It accepts an optional argument
  which specifies the logging level. By default, its logging level is `info`.

    **Warning:** due to internal issue, you should **NOT** output anything to
    `sys.stdout` or `sys.stderr`; instead, you should use this method to log anything you want.

* In order to declare the output fields of this bolt, you need to place
a class attribute `outputs` as a list of `str` or `Stream`. Note that unlike Java,
`declareOutputFields` does not exist in the Python implementation. Moreover, you can
optionally specify the output fields from the `spec()` method from the `optional_outputs`.


* You will use the `spec()` method to define a topology and specify the location
of this bolt within the topology, as well as to give component-specific configurations.

The following is an example implementation of a bolt in Python.

```python
from collections import Counter
from heronpy.api.bolt.bolt import Bolt


class CountBolt(Bolt):
    outputs = ["word", "count"]

    def initialize(self, config, context):
        self.counter = Counter()

    def process(self, tup):
        word = tup.values[0]
        self.counter[word] += 1
        self.emit([word, self.counter[word]])
```

## Spouts

To create a spout for a Heron topology, you need to subclass the [`Spout`](/api/python/spout/spout.m.html#heronpy.spout.spout.Spout) class, which has the following methods.

```python
class MySpout(Spout):
    def initialize(self, config, context): pass
    def next_tuple(self): pass
    def ack(self, tup_id): pass
    def fail(self, tup_id): pass
    def activate(self): pass
    def deactivate(self): pass
    def close(self): pass
```

## `Spout` class methods

The [`Spout`](/api/python/spout/spout.m.html#heronpy.spout.spout.Spout) class provides a number of methods that you should implement when subclassing.

* The `initialize()` method is called when the spout is first initialized
and provides the spout with the executing environment. It is equivalent to
`open()` method of [`ISpout`](/api/org/apache/heron/api/spout/ISpout.html).
Note that you should not override `__init__()` constructor of `Spout` class
for initialization of custom variables, since it is used internally by HeronInstance; instead,
`initialize()` should be used to initialize any custom variables or connections to databases.

* The `next_tuple()` method is used to fetch tuples from input source. You can
emit fetched tuples by calling `self.emit()`, as described below.

* The `ack()` method is called when the `HeronTuple` with the `tup_id` emitted
by this spout is successfully processed.

* The `fail()` method is called when the `HeronTuple` with the `tup_id` emitted
by this spout is not processed successfully.

* The `activate()` method is called when the spout is asked to back into
active state.

* The `deactivate()` method is called when the spout is asked to enter deactive
state.

* The `close()` method is called when when the spout is shutdown. There is no
guarantee that this method is called due to how the instance is killed.

## `BaseSpout` class methods

The `Spout` class inherits from the [`BaseSpout`](/api/python/spout/base_spout.m.html#heronpy.spout.base_spout.BaseSpout) class, which also provides you methods you can use in your spouts.

```python
class BaseSpout(BaseComponent):
    def log(self, message, level=None): ...
    def emit(self, tup, tup_id=None, stream="default", direct_task=None, need_task_ids=False): ...
    @classmethod
    def spec(cls, name=None, par=1, config=None): ...
```

* The `emit()` method is used to emit a given tuple, which can be a `list` or `tuple` of any Python objects. Unlike in the Java implementation, there is no `OutputCollector` in the Python implementation.

* The `log()` method is used to log an arbitrary message, and its outputs are redirected to the log file of the component. It accepts an optional argument which specifies the logging level. By default, its logging level is `info`.

    **Warning:** due to internal issue, you should **NOT** output anything to
    `sys.stdout` or `sys.stderr`; instead, you should use this method to log anything you want.

* In order to declare the output fields of this spout, you need to place
a class attribute `outputs` as a list of `str` or `Stream`. Note that unlike Java,
`declareOutputFields` does not exist in the Python implementation. Moreover, you can
optionally specify the output fields from the `spec()` method from the `optional_outputs`.
.

* You will use the `spec()` method to define a topology and specify the location
of this spout within the topology, as well as to give component-specific configurations.

## Example spout

The following is an example implementation of a spout in Python.

```python
from itertools import cycle
from heronpy.api.spout.spout import Spout


class WordSpout(Spout):
    outputs = ['word']

    def initialize(self, config, context):
        self.words = cycle(["hello", "world", "heron", "storm"])
        self.log("Initializing WordSpout...")

    def next_tuple(self):
        word = next(self.words)
        self.emit([word])
```

## Topologies Further

```shell
$ pip install heronpy
$ easy_install heronpy
```

Then you can include `heronpy` in your project files. Here's an example:

```python
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.spout.spout import Spout
from heronpy.api.topology import Topology
```

## Writing topologies in Python

Heron [topologies](heron-topologies-concepts) are networks of [spouts](../spouts) that pull data into a topology and [bolts](../bolts) that process that ingested data.

> You can see how to create Python spouts in the [Implementing Python Spouts](../spouts) guide and how to create Python bolts in the [Implementing Python Bolts](../bolts) guide.

Once you've defined spouts and bolts for a topology, you can then compose the topology in one of two ways:

* You can use the [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder) class inside of a main function.

    Here's an example:

    ```python
    #!/usr/bin/env python
    from heronpy.api.topology import TopologyBuilder


    if __name__ == "__main__":
        builder = TopologyBuilder("MyTopology")
        # Add spouts and bolts
        builder.build_and_submit()
    ```

* You can subclass the [`Topology`](/api/python/topology.m.html#heronpy.topology.Topology) class.

    Here's an example:

    ```python
    from heronpy.api.stream import Grouping
    from heronpy.api.topology import Topology


    class MyTopology(Topology):
        my_spout = WordSpout.spec(par=2)
        my_bolt = CountBolt.spec(par=3, inputs={spout: Grouping.fields("word")})
    ```

## Defining topologies using the [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder) class

If you create a Python topology using a [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder), you need to instantiate a `TopologyBuilder` inside of a standard Python main function, like this:

```python
from heronpy.api.topology import TopologyBuilder


if __name__ == "__main__":
    builder = TopologyBuilder("MyTopology")
```

Once you've created a `TopologyBuilder` object, you can add [bolts](../bolts) using the [`add_bolt`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder.add_bolt) method and [spouts](../spouts) using the [`add_spout`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder.add_spout) method. Here's an example:

```python
builder = TopologyBuilder("MyTopology")
builder.add_bolt("my_bolt", CountBolt, par=3)
builder.add_spout("my_spout", WordSpout, par=2)
```

Both the `add_bolt` and `add_spout` methods return the corresponding [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec) object.

The `add_bolt` method takes four arguments and an optional `config` parameter:

Argument | Data type | Description | Default
:--------|:----------|:------------|:-------
`name` | `str` | The unique identifier assigned to this bolt | |
`bolt_cls` | class | The subclass of [`Bolt`](/api/python/bolt/bolt.m.html#heronpy.bolt.bolt.Bolt) that defines this bolt | |
`par` | `int` | The number of instances of this bolt in the topology | |
`config` | `dict` | Specifies the configuration for this spout | `None`

The `add_spout` method takes three arguments and an optional `config` parameter:

Argument | Data type | Description | Default
:--------|:----------|:------------|:-------
`name` | `str` | The unique identifier assigned to this spout | |
`spout_cls` | class | The subclass of [`Spout`](/api/python/spout/spout.m.html#heronpy.spout.spout.Spout) that defines this spout | |
`par` | `int` | The number of instances of this spout in the topology | |
`inputs` | `dict` or `list` | Either a `dict` mapping from [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec) to [`Grouping`](/api/python/stream.m.html#heronpy.stream.Grouping) *or* a list of [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec)s, in which case the [`shuffle`](/api/python/stream.m.html#heronpy.stream.Grouping.SHUFFLE) grouping is used
`config` | `dict` | Specifies the configuration for this spout | `None`

### Example

The following is an example implementation of a word count topology in Python that subclasses [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder).

```python
from your_spout import WordSpout
from your_bolt import CountBolt

from heronpy.api.stream import Grouping
from heronpy.api.topology import TopologyBuilder


if __name__ == "__main__":
    builder = TopologyBuilder("WordCountTopology")
    # piece together the topology
    word_spout = builder.add_spout("word_spout", WordSpout, par=2)
    count_bolt = builder.add_bolt("count_bolt", CountBolt, par=2, inputs={word_spout: Grouping.fields("word")})
    # submit the toplogy
    builder.build_and_submit()
```

Note that arguments to the main method can be passed by providing them in the
`heron submit` command.

### Topology-wide configuration

If you're building a Python topology using a `TopologyBuilder`, you can specify configuration for the topology using the [`set_config`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder.set_config) method. A topology's config is a `dict` in which the keys are a series constants from the [`api_constants`](/api/python/api_constants.m.html) module and values are configuration values for those parameters.

Here's an example:

```python
from heronpy.api import api_constants
from heronpy.api.topology import TopologyBuilder


if __name__ == "__main__":
    topology_config = {
        api_constants.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS: True
    }
    builder = TopologyBuilder("MyTopology")
    builder.set_config(topology_config)
    # Add bolts and spouts, etc.
```

### Launching the topology

If you want to [submit](../../../operators/heron-cli#submitting-a-topology) Python topologies to a Heron cluster, they need to be packaged as a [PEX](https://pex.readthedocs.io/en/stable/whatispex.html) file. In order to produce PEX files, we recommend using a build tool like [Pants](http://www.pantsbuild.org/python_readme.html) or [Bazel](https://github.com/benley/bazel_rules_pex).

If you defined your topology by subclassing the [`TopologyBuilder`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder) class and built a `word_count.pex` file for that topology in the `~/topology` folder. You can submit the topology to a cluster called `local` like this:

```bash
$ heron submit local \
  ~/topology/word_count.pex \
  - # No class specified
```

Note the `-` in this submission command. If you define a topology by subclassing `TopologyBuilder` you do not need to instruct Heron where your main method is located.

> #### Example topologies buildable as PEXs
> * See [this repo](https://github.com/streamlio/pants-dev-environment) for an example of a Heron topology written in Python and deployable as a Pants-packaged PEX.
> * See [this repo](https://github.com/streamlio/bazel-dev-environment) for an example of a Heron topology written in Python and deployable as a Bazel-packaged PEX.

## Defining a topology by subclassing the [`Topology`](/api/python/topology.m.html#heronpy.topology.Topology) class

If you create a Python topology by subclassing the [`Topology`](/api/python/topology.m.html#heronpy.topology.Topology) class, you need to create a new topology class, like this:

```python
from my_spout import WordSpout
from my_bolt import CountBolt

from heronpy.api.stream import Grouping
from heronpy.api.topology import Topology


class MyTopology(Topology):
    my_spout = WordSpout.spec(par=2)
    my_bolt_inputs = {my_spout: Grouping.fields("word")}
    my_bolt = CountBolt.spec(par=3, inputs=my_bolt_inputs)
```

All you need to do is place [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec)s as the class attributes
of your topology class, which are returned by the `spec()` method of
your spout or bolt class. You do *not* need to run a `build` method or anything like that; the `Topology` class will automatically detect which spouts and bolts are included in the topology.

> If you use this method to define a new Python topology, you do *not* need to have a main function.

For bolts, the [`spec`](/api/python/bolt/bolt.m.html#heronpy.bolt.bolt.Bolt.spec) method for spouts takes three optional arguments::

Argument | Data type | Description | Default
:--------|:----------|:------------|:-------
`name` | `str` | The unique identifier assigned to this bolt or `None` if you want to use the variable name of the return `HeronComponentSpec` as the unique identifier for this bolt | |
`par` | `int` | The number of instances of this bolt in the topology | |
`config` | `dict` | Specifies the configuration for this bolt | `None`


For spouts, the [`spec`](/api/python/spout/spout.m.html#heronpy.spout.spout.Spout.spec) method takes four optional arguments:

Argument | Data type | Description | Default
:--------|:----------|:------------|:-------
`name` | `str` | The unique identifier assigned to this spout or `None` if you want to use the variable name of the return `HeronComponentSpec` as the unique identifier for this spout | `None` |
`inputs` | `dict` or `list` | Either a `dict` mapping from [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec) to [`Grouping`](/api/python/stream.m.html#heronpy.stream.Grouping) *or* a list of [`HeronComponentSpec`](/api/python/component/component_spec.m.html#heronpy.component.component_spec.HeronComponentSpec)s, in which case the [`shuffle`](/api/python/stream.m.html#heronpy.stream.Grouping.SHUFFLE) grouping is used
`par` | `int` | The number of instances of this spout in the topology | `1` |
`config` | `dict` | Specifies the configuration for this spout | `None`

### Example

Here's an example topology definition with one spout and one bolt:

```python
from my_spout import WordSpout
from my_bolt import CountBolt

from heronpy.api.stream import Grouping
from heronpy.api.topology import Topology


class WordCount(Topology):
    word_spout = WordSpout.spec(par=2)
    count_bolt = CountBolt.spec(par=2, inputs={word_spout: Grouping.fields("word")})
```

### Launching

If you defined your topology by subclassing the [`Topology`](/api/python/topology.m.html#heronpy.topology.Topology) class,
your main Python file should *not* contain a main method. You will, however, need to instruct Heron which class contains your topology definition.

Let's say that you've defined a topology by subclassing `Topology` and built a PEX stored in `~/topology/dist/word_count.pex`. The class containing your topology definition is `topology.word_count.WordCount`. You can submit the topology to a cluster called `local` like this:

```bash
$ heron submit local \
  ~/topology/dist/word_count.pex \
  topology.word_count.WordCount \ # Specifies the topology class definition
  WordCountTopology
```

### Topology-wide configuration

If you're building a Python topology by subclassing `Topology`, you can specify configuration for the topology using the [`set_config`](/api/python/topology.m.html#heronpy.topology.TopologyBuilder.set_config) method. A topology's config is a `dict` in which the keys are a series constants from the [`api_constants`](/api/python/api_constants.m.html) module and values are configuration values for those parameters.

Here's an example:

```python
from heronpy.api.topology import Topology
from heronpy.api import api_constants


class MyTopology(Topology):
    config = {
        api_constants.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS: True
    }
    # Add bolts and spouts, etc.
```

## Multiple streams

To specify that a component has multiple output streams, instead of using a list of
strings for `outputs`, you can specify a list of `Stream` objects, in the following manner.

```python
class MultiStreamSpout(Spout):
    outputs = [
        Stream(fields=["normal", "fields"], name="default"),
        Stream(fields=["error_message"], name="error_stream"),
    ]
```

To select one of these streams as the input for your bolt, you can simply
use `[]` to specify the stream you want. Without any stream specified, the `default`
stream will be used.

```python
class MultiStreamTopology(Topology):
    spout = MultiStreamSpout.spec()
    error_bolt = ErrorBolt.spec(inputs={spout["error_stream"]: Grouping.LOWEST})
    consume_bolt = ConsumeBolt.spec(inputs={spout: Grouping.SHUFFLE})
```

## Declaring output fields using the `spec()` method

In Python topologies, the output fields of your spouts and bolts
need to be declared by placing `outputs` class attributes, as there is
no `declareOutputFields()` method. `heronpy` enables you to dynamically declare output fields as a list using the
`optional_outputs` argument in the `spec()` method.

This is useful in a situation like below.

```python
class IdentityBolt(Bolt):
    # Statically declaring output fields is not allowed
    class process(self, tup):
        emit([tup.values])


class DynamicOutputField(Topology):
    spout = WordSpout.spec()
    bolt = IdentityBolt.spec(inputs={spout: Grouping.ALL}, optional_outputs=["word"])
```

You can also declare outputs in the `add_spout()` and the `add_bolt()`
method for the `TopologyBuilder` in the same way.

## Example topologies

There are a number of example topologies that you can peruse in the [`examples/src/python`]({{% githubMaster %}}/examples/src/python) directory of the [Heron repo]({{% githubMaster %}}):

Topology | File | Description
:--------|:-----|:-----------
Word count | [`word_count_topology.py`]({{% githubMaster %}}/examples/src/python/word_count_topology.py) | The [`WordSpout`]({{% githubMaster %}}/examples/src/python/spout/word_spout.py) spout emits random words from a list, while the [`CountBolt`]({{% githubMaster %}}/examples/src/python/bolt/count_bolt.py) bolt counts the number of words that have been emitted.
Multiple streams | [`multi_stream_topology.py`]({{% githubMaster %}}/examples/src/python/multi_stream_topology.py) | The [`MultiStreamSpout`]({{% githubMaster %}}/examples/src/python/spout/multi_stream_spout.py) emits multiple streams to downstream bolts.
Half acking | [`half_acking_topology.py`]({{% githubMaster %}}/examples/src/python/half_acking_topology.py) | The [`HalfAckBolt`]({{% githubMaster %}}/examples/src/python/bolt/half_ack_bolt.py) acks only half of all received tuples.
Custom grouping | [`custom_grouping_topology.py`]({{% githubMaster %}}/examples/src/python/custom_grouping_topology.py) | The [`SampleCustomGrouping`]({{% githubMaster %}}/examples/src/python/custom_grouping_topology.py#L26) class provides a custom field grouping.

You can build the respective PEXs for these topologies using the following commands:

```shell
$ bazel build examples/src/python:word_count
$ bazel build examples/src/python:multi_stream
$ bazel build examples/src/python:half_acking
$ bazel build examples/src/python:custom_grouping
```

All built PEXs will be stored in `bazel-bin/examples/src/python`. You can submit them to Heron like so:

```shell
$ heron submit local \
  bazel-bin/examples/src/python/word_count.pex - \
  WordCount
$ heron submit local \
  bazel-bin/examples/src/python/multi_stream.pex \
  heron.examples.src.python.multi_stream_topology.MultiStream
$ heron submit local \
  bazel-bin/examples/src/python/half_acking.pex - \
  HalfAcking
$ heron submit local \
  bazel-bin/examples/src/python/custom_grouping.pex \
  heron.examples.src.python.custom_grouping_topology.CustomGrouping
```

By default, the `submit` command also activates topologies. To disable this behavior, set the `--deploy-deactivated` flag.
