---
id: version-0.20.0-incubating-guides-tuple-serialization
title: Tuple Serialization
sidebar_label: Tuple Serialization
original_id: guides-tuple-serialization
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

The tuple is Heron's core data type. Heron's native
[`Tuple`](/api/org/apache/heron/api/tuple/Tuple.html) interface supports
a broad range of [basic data types](guides-data-model#using-tuples), such as
strings, integers, and booleans, out of the box, but tuples can contain values
of any type. You can use data types beyond the core types by providing a custom
serializer using the instructions below.

## Kryo

Heron uses [Kryo](https://github.com/EsotericSoftware/kryo) for tuple
serialization and deserialization. You can create a custom tuple serializer by
extending Kryo's abstract
[`Serializer`](http://code.google.com/p/kryo/source/browse/trunk/src/com/esotericsoftware/kryo/Serializer.java)
class. More information can be found in [Kryo's
documentation](https://github.com/EsotericSoftware/kryo#serializers).

## Registering a Serializer

Once you've created a custom Kryo serializer for a type:

1. Make sure that the code for the serializer is on Heron's
[classpath](../compiling/compiling/#classpath).
2. Register the class with Kryo using the `topology.kryo.register` parameter for
your topology. Here's an example:

  ```yaml
  topology.kryo.register:
    - biz.acme.heron.datatypes.CustomType1 # This type will use the default FieldSerializer
    - biz.acme.heron.datatypes.CustomType2: com.example.heron.serialization.CustomSerializer
  ```

Once your custom serializer is on Heron's classpath and Heron is aware of its
existence, you must [re-compile](compiling-overview) Heron.