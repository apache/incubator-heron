---
title: Writing and Launching a Topology in Python
---

Currently, support for developing a Heron topology in Python is still experimental.
It is compatible with the Streamparse API, so Python topologies written
for the Streamparse can be deployed on Heron with ease.
This page describes how to write and launch a topology in Python, as well as
how to convert a Streamparse topology to a PyHeron topology.

Note that a Python topology is known to be approximately 20-40 times slower
than a topology written in Java. This performance issue will be resolved in later releases.

You need to first download `PyHeron` library and include it in your project.

# Writing your own topology in Python

[Spouts](../spouts) and [Bolts](../bolts) discuss how to implement spouts and 
bolts in Python, respectively.

After defining the spouts and bolts, a topology can be composed by two ways:

* Using `TopologyBuilder` (not compatible with the Streamparse API)
* Subclassing `Topology` class (compatible with the Streamparse API)

## Defining a topology using a TopologyBuilder

This way of defining a topology is similar to defining a topology in Java,
and is not compatible with the Streamparse API.

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

The following is an example implementation of WordCountTopology in Python.

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

This way of defining a topology is compatible with the Streamparse API.
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

For further information about the API, refer to the Streamparse API documentation,
although there are some methods and functionalities that are not supported or
are invalid in Heron.

## Declaring output fields from the spec() method
In Python topologies, the `declareOutputFields()` method doesn't exist, so
the output fields of your spout and bolt need to be declared by placing
`outputs` class attributes. This is compatible with the Streamparse API, but
dynamically declaring output fields is more complicated in this way.
So, PyHeron provides a way to dynamically declare output fields via the
`optional_outputs` argument in the `spec()` method.

This is useful in a situation like below.

```python
class IdentityBolt(Bolt):
  # can't statically declare output fields
  class process(self, tup):
    emit([tup.values])
```

```python
class DynamicOutputField(Topology):
  spout = WordSpout.spec()
  bolt = IdentityBolt.spec(inputs={spout: Grouping.ALL},
                           optional_outputs=['word'])
```

You can also declare outputs in the `add_spout()` and the `add_bolt()`
method for the `TopologyBuilder` in the same way.

# Launching your python topology

You need to first package your Python topology project to a PEX file.

If you defined your topology using `TopologyBuilder`, your topology
definition python file should have `if __name__ = "__main__"` method.
The following shows the submission command of an example WordCountTopology, where its
pex file is located in `~/project/word_count.pex`.

```bash
$ heron submit local ~/project/word_count.pex - WordCountTopology
```

If you defined your topology by subclassing `Topology`, your topology
definition python file should not contain main method.
The following shows the submission command of an example WordCountTopology, where
its pex file is located in `~/project/word_count.pex`, inside which your `WordCount`
class resides under `topology.word_count_topology.WordCount`.

```bash
$ heron submit local \
~/project/word_count.pex \
topology.word_count_topology.WordCount \
WordCountTopology
```
