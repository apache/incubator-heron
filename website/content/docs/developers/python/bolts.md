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
---
title: Implementing Python bolts
---

> #### Python API docs
> You can find API docs for the [`heronpy`](https://pypi.python.org/pypi/heronpy) library [here](/api/python).

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
For further information, refer to [this page](../topologies).


* You will use the `spec()` method to define a topology and specify the location
of this bolt within the topology, as well as to give component-specific configurations.
For the usage of this method, refer to [this page](../topologies).

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
