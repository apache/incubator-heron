---
title: Tuple Serialization
---

The tuple is Heron's core data type. Heron's native
[`Tuple`](/api/com/twitter/heron/api/tuple/Tuple.html) interface supports
a broad range of [basic data types](../data-model/#using-tuples), such as
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
existence, you must [re-compile](../compiling/compiling) Heron.
