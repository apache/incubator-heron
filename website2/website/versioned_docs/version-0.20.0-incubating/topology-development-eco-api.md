---
id: version-0.20.0-incubating-topology-development-eco-api
title: The ECO API for Java
sidebar_label: The ECO API for Java
original_id: topology-development-eco-api
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

> **The Heron ECO API is in beta**. The Heron ECO API can be used to build and test topologies on your local or on a cluster.  The API still needs some testing and feedback from the community to understand how we  should continue to develop ECO.

Heron processing topologies can be written using an API called the **Heron ECO API**. The ECO API is currently available to work with spouts and bolts from the following packages:

* `org.apache.storm`
* `org.apache.heron`

> Although this document focuses on the ECO API, both the [Streamlet API](heron-streamlet-concepts) and [Topology API](heron-topology-concepts) topologies you have built can still be used with Heron

## The Heron ECO API vs. The Streamlet and Topology APIs

Heron's ECO offers one major difference over the Streamlet and Topology APIs and that is extensibility without recompilation.
With Heron's ECO developers now have a way to alter the way data flows through spouts and bolts without needing to get into their code and make changes.
Topologies can now be defined through a YAML based format.

## Why the name ECO?

/ˈekoʊ/ (Because all software should come with a pronunciation guide these days)
ECO is an acronym that stands for:
* Extensible
* Component
* Orchestrator


## What about Storm Flux?  Is it compatible with Eco?

ECO is an extension of Flux.  Most Storm Flux topologies should be able to deployed in Heron with minimal changes.
Start reading [Migrate Storm Topologies To Heron] (../../../migrate-storm-to-heron) to learn how to migrate your Storm Flux topology then come back.

## Getting started

In order to use the Heron ECO API for Java, you'll need to install the `heron-api` and the `heron-storm` library, which is available
via [Maven Central](http://search.maven.org/).

### Maven setup

To install the `heron-api` library using Maven, add this to the `dependencies` block of your `pom.xml`
configuration file:

```xml
<dependency>
    <groupId>org.apache.heron</groupId>
    <artifactId>heron-api</artifactId>
    <version>{{< heronVersion >}}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>org.apache.heron</groupId>
    <artifactId>heron-storm</artifactId>
    <version>{{< heronVersion >}}</version>
    <scope>compile</scope>
</dependency>
```

#### Compiling a JAR with dependencies

In order to run a Java topology in a Heron cluster, you'll need to package your topology as a "fat" JAR with dependencies included. You can use the [Maven Assembly Plugin](https://maven.apache.org/plugins/maven-assembly-plugin/usage.html) to generate JARs with dependencies. To install the plugin and add a Maven goal for a single JAR, add this to the `plugins` block in your `pom.xml`:

```xml
<plugin>
    <artifactId>maven-assembly-plugin</artifactId>
    <configuration>
        <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
        <archive>
            <manifest>
                <mainClass></mainClass>
            </manifest>
        </archive>
    </configuration>
    <executions>
        <execution>
            <id>make-assembly</id>
            <phase>package</phase>
            <goals>
                <goal>single</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

Once your `pom.xml` is properly set up, you can compile the JAR with dependencies using this command:

```bash
$ mvn assembly:assembly
```

By default, this will add a JAR in your project's `target` folder with the name `PROJECT-NAME-VERSION-jar-with-dependencies.jar`. Here's an example ECO topology submission command using a compiled JAR:

```bash
$ heron submit local \
  target/my-project-1.2.3-jar-with-dependencies.jar \
  org.apache.heron.eco.Eco \
  --eco-config-file path/to/your/topology-definition.yaml
```

### Reference Links
[Topology Name](#topology-name)

[Configuration](#configuration)

[Components](#components)

[Property Injection](#property-injection)

[The Topology Definition](#the-topology-definition)

[Streams and Groupings](#streams-and-groupings)

[Handling Enums](#handling-enums)

[Property Substitution](#property-substitution)

[Environment Variable Substitution](#environment-variable-substitution)

[Other ECO Examples](#other-eco-examples)

Notice how the above example submission command is referencing the main class `org.apache.heron.eco.Eco`.  This part of the command
needs to stay the same.  Eco is the main class that will assemble your topology from the `--eco-config-file` you specify.

## Defining Your ECO Topology File

An ECO topology definition consists of the following:

* A topology name
* An optional list of topology "components" (named Java objects that will be made available for configuration in the topology)
* A DSL topology definition that contains:
  - A list of spouts, each identified by a unique ID
  - A list of bolts, each identified by a unique ID
  - A list of "stream" objects representing a flow of tuples between spouts and bolts


An example of a simple YAML DSL definition is below:

```yaml

name: "fibonacci-topology"

config:
  topology.workers: 1

components:
  - id: "property-holder"
    className: "org.apache.heron.examples.eco.TestPropertyHolder"
    constructorArgs:
      - "some argument"
    properties:
      - name: "numberProperty"
        value: 11
      - name: "publicProperty"
        value: "This is public property"

spouts:
  - id: "spout-1"
    className: "org.apache.heron.examples.eco.TestFibonacciSpout"
    constructorArgs:
      - ref: "property-holder"
    parallelism: 1

bolts:
  - id: "even-and-odd-bolt"
    className: "org.apache.heron.examples.eco.EvenAndOddBolt"
    parallelism: 1

  - id: "ibasic-print-bolt"
    className: "org.apache.heron.examples.eco.TestIBasicPrintBolt"
    parallelism: 1
    configMethods:
      - name: "sampleConfigurationMethod"
        args:
          - "${ecoPropertyOne}"
          - MB

  - id: "sys-out-bolt"
    className: "org.apache.heron.examples.eco.TestPrintBolt"
    parallelism: 1

streams:
  - from: "spout-1"
    to: "even-and-odd-bolt"
    grouping:
      type: SHUFFLE

  - from: "even-and-odd-bolt"
    to: "ibasic-print-bolt"
    grouping:
      type: SHUFFLE
      streamId: "odds"

  - from: "even-and-odd-bolt"
    to: "sys-out-bolt"
    grouping:
      type: SHUFFLE
      streamId: "evens"

```

If you want to stop here and try to deploy the above topology you can execute:

```bash
$ heron submit local \
  ~/.heron/examples/heron-eco-examples.jar \
  org.apache.heron.eco.Eco \
  --eco-config-file ~/.heron/examples/storm_fibonacci.yaml
```

This ECO topology does not do anything spectacular, but it's a good starting point to go through some of ECO's concepts.

## Taking a closer look at the YAML definition specs

### Topology Name

Each ECO definition file will be required to have a `name` defined.

```yaml

name: "simple-wordcount-topology"

```

### Configuration

`config` is the section where you will list your properties to be inserted into a `org.apache.heron.api.Config` class. This section is optional.

```yaml

config:
  topology.workers: 1

```

#### Specifying Component Level Resources

You can specify component level JVM resources by referencing the `id` of the component and its `ram`.
You can choose between Bytes `B`, Megabytes `MB`, or Gigabytes `GB`.  Examples would be `256MB` or `2GB`.
The unit of measurement must be appended at the end of the numerical value with no spaces.  There is plan to support 
component level `cpu` and `disk` configs in the future.

```yaml
 topology.component.resourcemap:

    - id: "spout-1"
      ram: 256MB # The minimum value for a component's specified RAM is 256MB
      

    - id: "bolt-1"
      ram: 256MB # The minimum value for a component's specified RAM is 256MB
      
 ```
 
 #### Specifying JVM Options
 
 You can specify component level JVM resources by referencing the `id` of the component and a list
 of the JVM options
 
```yaml
topology.component.jvmoptions:

   - id: "spout-1"
     options: ["-XX:NewSize=300m", "-Xms2g"]
```

#### Other Supported Configuration Parameters
* `"topology.worker.childopts"` : Topology-specific options for the worker child process. This is used in addition to WORKER_CHILDOPTS
* `"topology.tick.tuple.freq.ms"` :  How often (in milliseconds) a tick tuple from the "__system" component and "__tick" stream should be sent to tasks. Meant to be used as a component-specific configuration.
* `"topology.enable.message.timeouts"` : True if Heron should timeout messages or not. Defaults to true. This is meant to be used in unit tests to prevent tuples from being accidentally timed out during the test.
* `"topology.debug"` : When set to true, Heron will log every message that's emitted.
*  `"topology.stmgrs"` : The number of stmgr instances that should spin up to service this topology. All the executors will be evenly shared by these stmgrs.
* `"topology.message.timeout.secs"` : The maximum amount of time given to the topology to fully process a message
emitted by a spout. If the message is not acked within this time frame, Heron
will fail the message on the spout. Some spouts implementations will then replay
the message at a later time.
* `"topology.component.parallelism"` : The per component parallelism for a component in this topology.
* `"topology.max.spout.pending"` : This config applies to individual tasks, not to spouts or topologies as a whole.
 A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
 Note that this config parameter has no effect for unreliable spouts that don't tag
 their tuples with a message id.
* `"topology.auto.task.hooks"` :  A list of task hooks that are automatically added to every spout and bolt in the topology. An example
 of when you'd do this is to add a hook that integrates with your internal
monitoring system. These hooks are instantiated using the zero-arg constructor.
* `"topology.serializer.classname"` : The serialization class that is used to serialize/deserialize tuples
* `"topology.reliability.mode"` : A Heron topology can be run in any one of the TopologyReliabilityMode
 mode. The format of this flag is the string encoded values of the
underlying TopologyReliabilityMode value.  Values are `ATMOST_ONCE`, `ATLEAST_ONCE`, and `EFFECTIVELY_ONCE`.
* `"topology.reliability.mode"` :  A Heron topology can be run in any one of the TopologyReliabilityMode
mode. The format of this flag is the string encoded values of the
underlying TopologyReliabilityMode value.
* `"topology.container.cpu"` : Number of CPU cores per container to be reserved for this topology.
* `"topology.container.ram"` : Amount of RAM per container to be reserved for this topology. In bytes.
* `"topology.container.disk"` : Amount of disk per container to be reserved for this topology. In bytes.
* `"topology.container.max.cpu.hint"` : Hint for max number of CPU cores per container to be reserved for this topology.
* `"topology.container.max.ram.hint"` : Hint for max amount of RAM per container to be reserved for this topology.  In bytes.
* `"topology.container.max.disk.hint"` : Hint for max amount of disk per container to be reserved for this topology. In bytes.
* `"topology.container.padding.percentage"` : Hint for max amount of disk per container to be reserved for this topology. In bytes.
* `"topology.container.ram.padding"` : Amount of RAM to pad each container. In bytes.
* `"topology.stateful.checkpoint.interval.seconds"` : What's the checkpoint interval for stateful topologies in seconds.
* `"topology.stateful.start.clean"` :  Boolean flag that says that the stateful topology should start from clean state, i.e. ignore any checkpoint state.
* `"topology.name"` :  Name of the topology. This config is automatically set by Heron when the topology is submitted.
* `"topology.team.name"` : Name of the team which owns this topology.
* `"topology.team.email"` : Email of the team which owns this topology.
* `"topology.cap.ticket"` :  Cap ticket (if filed) for the topology. If the topology is in prod this has to be set or it cannot be deployed.
* `"topology.project.name"` : Project name of the topology, to help us with tagging which topologies are part of which project. For example, if topology A and Topology B are part of the same project, we will like to aggregate them as part of the same project. This is required by Cap team.
* `"topology.additional.classpath"` :  Any user defined classpath that needs to be passed to instances should be set in to config through this key. The value will be of the format "cp1:cp2:cp3..."
* `"topology.update.deactivate.wait.secs"` : Amount of time to wait after deactivating a topology before updating it
* `"topology.update.reactivate.wait.secs"` : fter updating a topology, amount of time to wait for it to come back up before reactivating it
* `"topology.environment"` : Topology-specific environment properties to be added to an Heron instance. This is added to the existing environment (that of the Heron instance).  This variable contains Map<String, String>
* `"topology.timer.events"` : Timer events registered for a topology.  This is a Map<String, Pair<Duration, Runnable>>.  Where the key is the name and the value contains the frequency of the event and the task to run.
* `"topology.remote.debugging.enable"` : Enable Remote debugging for java heron instances
* `"topology.droptuples.upon.backpressure"` : Do we want to drop tuples instead of initiating Spout BackPressure
* `"topology.component.output.bps"` : The per component output bytes per second in this topology


### Components

`components` are a list of instances that would be used as configuration objects for other components in your ECO file defined in the YAML DSL.
The properties that are required for each `component` instance are `id` and `className`.  `id` can be any name you choose, `className`  is the fully qualified className of the Java class. The `id` field is used to identify
the component for injection in the coming spouts and bolts defined in the topology.  `constructorArgs` is only needed
if a component has constructor that requires arguments.  We will get into constructor args the in the next section.  `components` are optional.

```yaml

components:
  - id: "property-holder"
    className: "org.apache.heron.examples.eco.TestPropertyHolder"
    constructorArgs:
      - "some argument"

```

### Property Injection

#### Constructor Injection

`constructorArgs` can specify any object type.  Above you can see that the only constructor argument specified is a string
that contained "some argument".  If declaring a number, you may omit the parenthesis.

```yaml

 constructorArgs:
      - "some argument"
      - 123.45

```

The is also a way to reference other components as arguments by using `ref`.  In the example
below we are specifying an already defined component to be a constructor argument.  Any instance that is referenced by
`ref` must have already been defined in the ECO definition file before it is to be used.

```yaml

constructorArgs:
      - ref: "property-holder"

```

#### Setter and Public Field Injection

Besides constructor injection, you may also use setter methods. In the below example, ECO will take inspect the component
for setters that match the names and values provided.  If no setter is defined, it will then look for a public field to set the property.


```yaml

properties:
      - name: "numberProperty"
        value: 11
      - name: "publicProperty"
        value: "This is public property"

```

## The Topology Definition

Spouts and Bolts each have their own sections for defining in the ECO file.  They are extensions of `components` so they will
be allowed the same property injection methods above.  One difference is the `parallelism` property they contain, it sets the `parallelism`
property for each bolt or spout once the topology has been deployed into Heron.


### Spouts

```yaml

spouts:
  - id: "spout-1"
    className: "org.apache.heron.examples.eco.TestFibonacciSpout"
    constructorArgs:
      - ref: "property-holder"
    parallelism: 1

```

### Bolts

```yaml

bolts:
  - id: "even-and-odd-bolt"
    className: "org.apache.heron.examples.eco.EvenAndOddBolt"
    parallelism: 1

```

### Streams and Groupings

Streams are what connect your bolts and spouts together.  Stream Groupings are the specific way you are to connect those streams to the spouts and bolts.

##### A Stream can contain the following fields.

* `from` - references the `id` of the component where data is coming from
* `to` - references the `id` of the component where the data is going
* `grouping` - This is grouping definition of how this stream will connect the two afore mentioned components together

##### A grouping can contain the following fields
* `type` - The type of grouping.
  - `SHUFFLE`
  - `FIELDS`
  - `ALL`
  - `GLOBAL`
  - `NONE`
  - `CUSTOM`
* `args` is specific to the `FIELDS` grouping type. You would specify this as a list like so `["arg1", "arg2"]`
* `customClass` if you wanted to create a custom grouping, you could specify the fully qualified class name here

In the below example, you can see the first Stream Definition declares that data will flow from `spout-1` to `even-and-odd-bolt` with a grouping type of `SHUFFLE`


```yaml

streams:
  - from: "spout-1"
    to: "even-and-odd-bolt"
    grouping:
      type: SHUFFLE

  - from: "even-and-odd-bolt"
    to: "ibasic-print-bolt"
    grouping:
      type: SHUFFLE
      streamId: "odds"

  - from: "even-and-odd-bolt"
    to: "sys-out-bolt"
    grouping:
      type: SHUFFLE
      streamId: "evens"

```

### Handling Enums

The usage of Enums is supported in ECO.  You can use enums in constructor arguments, references,
configuration methods, and properties  In the `fibonacci-topology` referenced above.  In the examples we reference the 
enum `TestUnits`.  The enum is shown below.

```java
public enum TestUnits {
  MB("MB"),
  GB("GB"),
  B("B");

  String value;

  TestUnits(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
```

In the `ibasic-print-bolt` a `configMethod` is specified with the name `sampleConfigurationMethod`.  

```yaml
  - id: "ibasic-print-bolt"
    ... excluded for simplicity
        args:
          - "someStringArgument"
          - MB
```
This is the same as calling the following java method 

```java

public void sampleConfigurationMethod(String someProperty, TestUnits TestUnits) {
    this.someProperty += someProperty;
    this.TestUnits = TestUnits;
  }
  
```

### Property Substitution

It's always nice to be able to define properties based on the environments you are in.  We haven't forgotten this with ECO. 
You are able to substitute values into your ECO files by either environment variables or a properties file.  
To turn on property substitution add the flag `--props` and specify a path to a `.properties` file. 

To start over run 
```bash
$ heron kill local fibonacci-topology
```
After the topology has been killed, execute:

```bash
$ heron submit local \
  ~/.heron/examples/heron-eco-examples.jar \
  org.apache.heron.eco.Eco \
  --eco-config-file ~/.heron/examples/fibonacci.yaml --props ~/.heron/examples/sample.properties
```

If you look above at the yaml file snippet at the beginning of the page you will see

```yaml
- id: "ibasic-print-bolt"
    className: "org.apache.heron.examples.eco.TestIBasicPrintBolt"
    parallelism: 1
    configMethods:
      - name: "sampleConfigurationMethod"
        args:
          - "${ecoPropertyOne}"
          - MB
 ```
In the  `sample.properties` we have a key value set at `ecoPropertyOne=thisValueWasSetFromAPropertiesFile`.  You can check the logs
for the `ibasic-print-bolt` and see the values are printing out.

### Environment Variable Substitution

ECO also allows you to do environment variable substitution.  To activate environment variable substitution pass the flag `--env-props` upon submitting a topology. If you have `SOME_VARIABLE` defined
you can reference in your yaml file like below.

```yaml
${ENV-SOME_VARIABLE}
```


### Other ECO examples

Run the simple wordcount example

```bash
$ heron submit local \
  ~/.heron/examples/heron-eco-examples.jar \
   org.apache.heron.eco.Eco \
   --eco-config-file ~/.heron/examples/storm_wordcount.yaml
```

Run the simple windowing example

```bash
$ heron submit local \
   ~/.heron/examples/heron-eco-examples.jar \
   org.apache.heron.eco.Eco \
   --eco-config-file ~/.heron/examples/storm_windowing.yaml
```
