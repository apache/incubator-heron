Heron processing topologies can be written using an API called the **Heron ECO API**. The ECO API is currently available to work with the following languages:

* [Java](../../../developers/java/eco-api)

> Although this document focuses on the ECO API, both the [Streamlet API](../../../concepts/streamlet-api) and [topology API](../../../concepts/topologies) topologies you have built will can still be used with Heron

## The Heron ECO API vs. The Streamlet and Topology APIs

Heron's ECO offers one major difference over the Streamlet and Topology APIs and that is extensibility without recompilation.
With Heron's ECO developers now have a way to alter the way data flows through spouts and bolts without needing to get into their code and make changes.
Topologies can now be defined through a YAML based format.

## What does ECO stand for?
### /ˈekoʊ/ (Because all software should come with a pronunciation guide these days)

* Extensible
* Component
* Orchestrator


## Getting started

In order to use the Heron ECO for Java, you'll need to install the `heron-api` and the `heron-storm` library, which is available
via [Maven Central](http://search.maven.org/).

### Maven setup

To install the `heron-api` library using Maven, add this to the `dependencies` block of your `pom.xml`
configuration file:

```xml
<dependency>
    <groupId>com.twitter.heron</groupId>
    <artifactId>heron-api</artifactId>
    <version>{{< heronVersion >}}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.twitter.heron</groupId>
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
  com.twitter.heron.eco.Eco mySample \
  --eco-config-file path/to/your/topology-definition.yaml
```

## What about Storm Flux?  Is it compatible with Eco?

We built ECO with Flux in mind.  Most Storm Flux topologies should be able to deployed in Heron with minimal changes.
Start reading [Migrate Storm Topologies To Heron] (../../../migrate-storm-to-heron) to learn how to migrate your Storm Flux topology then come back.

