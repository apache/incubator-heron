---
id: version-0.20.0-incubating-guides-data-model
title: Heron Data Model
sidebar_label: Heron Data Model
original_id: guides-data-model
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

Tuple is Heron's core data type. All
data that is fed into a Heron topology via
[spouts](../../concepts/topologies#spouts) and then processed by
[bolts](../../concepts/topologies#bolts) consists of tuples.

Heron has a [`Tuple`](/api/org/apache/heron/api/tuple/Tuple.html)
interface for working with tuples. Heron `Tuple`s can hold values of any type;
values are accessible either by providing an index or a field name.

## Using Tuples

Heron's `Tuple` interface contains the methods listed in the [Javadoc
definition](/api/org/apache/heron/api/tuple/Tuple.html).

### Accessing Primitive Types By Index

Heron `Tuple`s support a wide variety of primitive Java types, including
strings, Booleans, byte arrays, and more.
[`getString`](/api/org/apache/heron/api/tuple/Tuple.html#getString-int-)
method, for example, takes an integer index and returns either a string or
`null` if no string value is present at that index. Analogous methods can be
found in the Javadoc.

### Accessing Primitive Types By Field

In addition to being accessible via index, values stored in Heron tuples are
accessible via field name as well. The
[`getStringByField`](/api/org/apache/heron/api/tuple/Tuple.html#getStringByField-java.lang.String-)
method, for example, takes a field name string and returns either a string or
`null` if no string value is present for that field name. Analogous methods can
be found in the Javadoc.

### Using Non-primitive Types

In addition to primitive types, you can access any value in a Heron `Tuple` as a
Java `Object`. As for primitive types, you can access `Object`s on the basis of
an index or a field name. The following methods return either an `Object` or
`null` if no object is present:

* [`getValue`](/api/org/apache/heron/api/tuple/Tuple.html#getValue-int-)
* [`getValueByField`](/api/org/apache/heron/api/tuple/Tuple.html#getValueByField-java.lang.String-)

You can also retrieve all objects contained in a Heron `Tuple` as a Java
[List](https://docs.oracle.com/javase/8/docs/api/java/util/List.html) using the
[`getValues`](/api/org/apache/heron/api/tuple/Tuple.html#getValues--)
method.

### User-defined Types

You use Heron tuples in conjunction with more complex, user-defined types using
[type casting](http://www.studytonight.com/java/type-casting-in-java), provided
that you've created and registered a [custom serializer](../serialization) for the type.
Here's an example (which assumes that a serializer for the type
`Tweet` has been created and registered):

```java
public void execute(Tuple input) {
    // The following return null if no value is present or throws a
    // ClassCastException if type casting fails:
    Tweet tweet = (Tweet) input.getValue(0);
    List<Tweet> allTweets = input.getValues();
}
```

More info on custom serialization can be found in [Creating Custom Tuple
Serializers](guides-tuple-serialization).

### Fields

The `getFields` method returns a
[`Fields`](http://heronproject.github.io/topology-api/org/apache/heron/api/tuple/Fields)
object that contains all of the fields in the tuple. More on fields can be found
[below](#Fields).

### Other Methods

There are additional methods available for determining the size of Heron
`Tuple`s, extracting contextual information, and more. For a full listing of
methods, see the
[Javadoc](/api/org/apache/heron/api/tuple/Tuple.html).

## Fields

From the methods in the list above you can see that you can retrieve single
values from a Heron tuple on the basis of their index. You can also retrieve
multiple values using a
[`Fields`](/api/org/apache/heron/api/tuple/Fields.html) object,
which can be initialized either using varargs or a list of strings:

```java
// Using varargs
Fields fruits = new Fields("apple", "orange", "banana");

// Using a list of strings
List<String> fruitNames = new LinkedList<String>();
fruitNames.add("apple");
// Add "orange" and "banana" as well
Fields fruits = new Fields(fruitNames);
```

You can then use that object in conjunction with a tuple:

```java
public void execute(Tuple input) {
    List<Object> values = input.select(fruits);
    for (Object value : values) {
        System.out.println(value);
    }
}
```
