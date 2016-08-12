---
title: Implementing a Python Spout
---

Spouts must implement the `Spout` interface, which has the following methods.

```python
class Spout(BaseSpout):
  def initialize(self, config, context)
  def next_tuple(self)
  def ack(self, tup_id)
  def fail(self, tup_id)
  def activate(self)
  def deactivate(self)
  def close(self)
```

* The `initialize()` method is called when the spout is first initialized
and provides the spout with the executing environment. It is equivalent to
`open()` method of [`ISpout`](/api/com/twitter/heron/api/spout/ISpout.html).
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

In addition, `BaseSpout` class provides you with the following methods.

```python
class BaseSpout:
  def emit(self, tup, tup_id=None, stream="default", direct_task=None, need_task_ids=False)
 
  def log(self, message, level=None)

  @classmethod
  def spec(cls, name=None, par=1, config=None)
```

* The `emit()` method is used to emit a given `tup`, which can be a `list` or `tuple` of 
any python objects. Unlike the Java implementation, `OutputCollector`
doesn't exist in the Python implementation.

* The `log()` method is used to log an arbitrary message, and its outputs are redirected
  to the log file of the component. It accepts an optional argument
  which specifies the logging level. By default, its logging level is `info`. 

    **Warning:** due to internal issue, you should **NOT** output anything to
    `sys.stdout` or `sys.stderr`; instead, you should use this method to log anything you want. 

* In order to declare the output fields of this spout, you need to place
a class attribute `outputs` as a list of `str` or `Stream`. Note that unlike Java,
`declareOutputFields` does not exist in the Python implementation. Moreover, you can
optionally specify the output fields from the `spec()` method from the `optional_outputs`.
For further information, refer to [this page](../topologies).

* You will use the `spec()` method to define a topology and specify the location
of this spout within the topology, as well as to give component-specific configurations.
For the usage of this method, refer to [this page](../topologies).

For further information about the API, refer to the Streamparse API documentation,
although there are some methods in the Streamparse API that are not supported or are
invalid in Heron. Additionally, there are a number of example implementations 
under `heron/examples/src/python` directory.

The following is an example implementation of a spout in Python.

```python
from itertools import cycle
from pyheron import Spout

class WordSpout(Spout):
  outputs = ['word']
  
  def initialize(self, config, context):
    self.words = cycle(["hello", "world", "heron", "storm"])
    self.log("In initialize() of WordSpout)
  
  def next_tuple(self):
    word = next(self.words)
    self.emit([word])
```