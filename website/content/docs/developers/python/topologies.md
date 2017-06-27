---
title: Python Topologies
---

> #### Python API docs
> You can find API docs for the [PyHeron](https://pypi.python.org/pypi/pyheron) library [here](/api/python).

Support for developing Heron topologies in Python is provided by a Python library called [PyHeron](https://pypi.python.org/pypi/pyheron). Heron topology support in Python is compatible with Storm's [streamparse](https://streamparse.readthedocs.io/en/latest/api.html) API, which enables you to seamlessly deploy Python Storm topologies in Heron.

This page describes how to [write](#writing-your-own-topologies-in-python) and [launch](#launching-python-topologies) topologies in Python, as well as
how to convert a streamparse topology to a PyHeron topology.s

## Setup

First, you need to install the `PyHeron` library using [pip](https://pip.pypa.io/en/stable/), [EasyInstall](https://wiki.python.org/moin/EasyInstall), or an analogous tool:

```shell
$ pip install pyheron
$ easy_install pyheron
```

Then you can include it in your project files. Here's an example:

```python
from pyheron import Bolt, Spout, Topology
```

# Writing your own topologies in Python

Heron topologies are networks of *spouts* that pull data into a topology and *bolts* that process that ingested data. For a more comprehensive guide, see the [Heron Topology](/docs/concepts/topologies) doc.

> You can see how to create spouts in the [Implementing Python Spouts](../spouts) guide and how to create bolts in the [Implementing Python Bolts](../bolts) guide.

Once you've defined spouts and bolts for a topology, you can then compose the topology in one of two ways:

* You can use the [`TopologyBuilder`](/api/python/topology.m.html#pyheron.topology.TopologyBuilder) class, which is *not* compatible with the streamparse API
* You can subclass the [`Topology`](/api/python/topology.m.html#pyheron.topology.Topology) class, which *is* compatible with the streamparse API

## Defining topologies using the `TopologyBuilder` class

This way of defining a topology is similar to defining a topology in Java,
and is not compatible with the streamparse API.

> Topologies composed using `TopologyBuilder` are *not* compatible with Storm's streamparse API. To create topologies compatible with streamparse, see [below](#defining-topologies-using-the-topology-class)

The `TopologyBuilder` has two major methods to specify the components:

* `add_spout(self, name, spout_cls, par, config=None)`
  * `name` is `str` specifying the unique identifier that is assigned to this spout.
  * `spout_cls` is a subclass of `Spout` that defines this spout.
  * `par` is `int` specifying the number of instances of this spout.
  * `config` is `dict` specifying this spout-specific configuration.


* `add_bolt(self, name, bolt_cls, par, inputs, config=None)`
  * `name` is `str` specifying the unique identifier that is assigned to this bolt.
  * `bolt_cls` is a subclass of `Bolt` that defines this bolt.
  * `par` is `int` specifying the number of instances of this bolt.
  * `inputs` is either `dict` mapping from `HeronComponentSpec` to `Grouping`;
  or `list` of `HeronComponentSpec`, in which case the shuffle grouping is used.
  * `config` is `dict` specifying this bolt-specific configuration.

Each method returns the corresponding `HeronComponentSpec` object.

The following is an example implementation of a word count topology in Python.

```python
from pyheron import TopologyBuilder
from your_spout import WordSpout
from your_bolt import CountBolt

if __name__ == "__main__":
  builder = TopologyBuilder("WordCountTopology")
  word_spout = builder.add_spout("word_spout", WordSpout, par=2)
  count_bolt = builder.add_bolt("count_bolt", CountBolt, par=2,
                                inputs={word_spout: Grouping.fields('word')})
  builder.build_and_submit()
```

Note that arguments to the main method can be passed by providing them in the
`heron submit` command.

## Defining a topology by subclassing Topology class

This way of defining a topology is compatible with the streamparse API.
All you need to do is to place `HeronComponentSpec` as the class attributes
of your topology class, which are returned by the `spec()` method of
your spout or bolt class.

* `Spout.spec(cls, name=None, par=1, config=None)`
  * `name` is either `str` specifying the unique identifier that is assigned to this spout, or
  `None` if you want to use the variable name of the returned `HeronComponentSpec` as
  the unique identifier for this spout.
  * `par` is `int` specifying the number of instances of this spout.
  * `config` is `dict` specifying this spout-specific configuration.

* `Bolt.spec(cls, name=None, inputs=None, par=1, config=None)`
  * `name` is either `str` specifying the unique identifier that is assigned to this bolt; or
  `None` if you want to use the variable name of the returned `HeronComponentSpec` as
  the unique identifier for this bolt.
  * `inputs` is either `dict` mapping from `HeronComponentSpec` to `Grouping`;
  or `list` of `HeronComponentSpec`, in which case the shuffle grouping is used.
  * `par` is `int` specifying the number of instances of this bolt.
  * `config` is `dict` specifying this bolt-specific configuration.

The same WordCountTopology is defined in the following manner.

```python
from pyheron import Topology
from your_spout import WordSpout
from your_bolt import CountBolt

class WordCount(Topology):
  word_spout = WordSpout.spec(par=2)
  count_bolt = CountBolt.spec(par=2, inputs={word_spout: Grouping.fields('word')})
```

## Topology-wide configuration
Topology-wide configuration can be specified by using `set_config()` method if
you are using `TopologyBuilder`, or by placing `config` containing `dict`
as the class attribute of your topology. Note that these configuration will be
overriden by component-specific configuration at runtime

## Multiple streams
To specify that a component has multiple output streams, instead of using a list of
strings for `outputs`, you can specify a list of `Stream` objects, in the following manner.

```python
class MultiStreamSpout(Spout):
  outputs = [Stream(fields=['normal', 'fields'], name='default'),
             Stream(fields=['error_message'], name='error_stream')]
```

To select one of these streams as the input for your bolt, you can simply
use `[]` to specify the stream you want. Without any stream specified, the `default`
stream will be used.

```python
class MultiStreamTopology(Topology):
  spout = MultiStreamSpout.spec()
  error_bolt = ErrorBolt.spec(inputs={spout['error_stream']: Grouping.LOWEST})
  consume_bolt = ConsumeBolt.spec(inputs={spout: Grouping.SHUFFLE})
```

For further information about the API, refer to the streamparse API documentation,
although there are some methods and functionalities that are not supported or
are invalid in Heron.

## Declaring output fields using the `spec()` method

In Python topologies, so the output fields of your spout and bolt
need to be declared by placing`outputs` class attributes, as there is
no `declareOutputFields()` method. This is compatible with the streamparse
API, but dynamically declaring output fields is more complicated in this way.

PyHeron enables you to dynamically declare output fields as a list using the
`optional_outputs` argument in the `spec()` method.

This is useful in a situation like below.

```python
class IdentityBolt(Bolt):
  # Statically declaring output fields is not allowed
  class process(self, tup):
    emit([tup.values])

class DynamicOutputField(Topology):
  spout = WordSpout.spec()
  bolt = IdentityBolt.spec(inputs={spout: Grouping.ALL},
                           optional_outputs=['word'])
```

You can also declare outputs in the `add_spout()` and the `add_bolt()`
method for the `TopologyBuilder` in the same way.

## Launching Python topologies

If you want to [submit](../../../operators/heron-cli#submitting-a-topology) Python topologies to a Heron cluster, they need to be packaged as a [PEX](https://pex.readthedocs.io/en/stable/whatispex.html) file. In order to produce PEX files, we recommend using a build tool like [Pants](http://www.pantsbuild.org/python_readme.html) or [Bazel](https://github.com/benley/bazel_rules_pex).

> #### Example topology using Python and Pants
> See [this repo](https://github.com/streamlio/pants-dev-environment) for an example of a Heron topology written in Python and deployable as a Pants-packaged PEX.

### Topologies subclassing `TopologyBuilder`

If you defined your topology by subclassing the [`TopologyBuilder`](/api/python/topology.m.html#pyheron.topology.TopologyBuilder) class, your topology's main Python file should have a function like this:

```python
if __name__ == '__main__':
    builder = TopologyBuilder("WordCountTopology")
    # etc.
```

Let's say that you've used this method (subclassing `TopologyBuilder`) and built a `word_count.pex` file for that topology in the `~/topology/dist` folder. You can submit the topology to a cluster called `local` like this:

```bash
$ heron submit local \
  ~/topology/dist/word_count.pex \
  - \ # No class specified
  WordCountTopology
```

Note the `-` in this submission command. If you define a topology by subclassing `TopologyBuilder` you do not need to instruct Heron where your main method is located.

### Topologies subclassing `Topology`

If you defined your topology by subclassing the [`Topology`](/api/python/topology.m.html#pyheron.topology.Topology) class,
your main Python file should *not* contain a main method. You will, however, need to instruct Heron which class contains your topology definition.

Let's say that you've defined a topology by subclassing `Topology` and built a PEX stored in `~/topology/dist/word_count.pex`. The class containing your topology definition is `topology.word_count.WordCount`. You can submit the topology to a cluster called `local` like this:

```bash
$ heron submit local \
  ~/topology/dist/word_count.pex \
  topology.word_count.WordCount \ # Specifies the class definition
  WordCountTopology
```

## Example topologies

There are a number of example topologies that you can peruse in the [`heron/examples/src/python`]({{% githubMaster %}}/heron/examples/src/python):

* [Word count]({{% githubMaster %}}/heron/examples/src/python/word_count_topology.py)
* [Multi stream]({{% githubMaster %}}/heron/examples/src/python/multi_stream_topology.py)
* [Half acking]({{% githubMaster %}}/heron/examples/src/python/half_acking_topology.py)
* [Custom grouping]({{% githubMaster %}}/heron/examples/src/python/custom_grouping_topology.py)

You can build the respective PEXes for these topologies using the following commands:

```shell
$ bazel build heron/examples/src/python:word_count
$ bazel build heron/examples/src/python:multi_stream
$ bazel build heron/examples/src/python:half_acking
$ bazel build heron/examples/src/python:custom_grouping
```

All built PEXes will be stored in `bazel-bin/heron/examples/src/python`. You can submit them to Heron like so:

```shell
$ heron submit local \
  bazel-bin/heron/examples/src/python/word_count.pex \
  - \
  WordCount
$ heron submit local \
  bazel-bin/heron/examples/src/python/multi_stream.pex \
  heron.examples.src.python.multi_stream_topology.MultiStream \
  MultiStream
$ heron submit local \
  bazel-bin/heron/examples/src/python/half_acking.pex \
  - HalfAcking
$ heron submit local \
  bazel-bin/heron/examples/src/python/custom_grouping.pex \
  heron.examples.src.python.custom_grouping_topology.CustomGrouping \
  CustomGrouping
```
