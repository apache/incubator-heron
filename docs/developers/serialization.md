# Custom Serialization

The tuple is Heron's core data type. Heron's native
[`Tuple`](.io/topology-api/com/twitter/heron/api/tuple/Tuple) interface supports
a broad range of [basic data types](data-model.html#using-tuples) out of the
box. Many use cases for topologies, however, will require you to provide
serialization and deserialization for other data types that are passed between
topology components.

## Kryo

Heron uses [Kryo](https://github.com/EsotericSoftware/kryo) for tuple
serialization and deserialization. You can create a custom tuple serializer by
extending Kryo's abstract
[`Serializer`](http://code.google.com/p/kryo/source/browse/trunk/src/com/esotericsoftware/kryo/Serializer.java)
class.

If you create a custom Kryo serializer, you'll need to do two things:

1. Make sure that the code for the serializer is on Heron's classpath.
2. Register the class with Kryo using the `topology.kryo.register` parameter.
Here's an example:

  ```yaml
  topology.kryo.register:
    - com.example.heron.datatypes.CustomType1 # This type will use the default FieldSerializer
    - com.example.heron.datatypes.CustomType2: com.example.heron.serialization.CustomSerializer
  ```
