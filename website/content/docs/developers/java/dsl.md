---
title: The Heron DSL for Java
description: Create Heron topologies in Java using a functional programming style
---

## Getting started

In order to use the Heron DSL for Java, you'll need to install the `heron-dsl` library.

### Maven setup

In order to use the `heron-dsl` library, add this to the `dependencies` block of your `pom.xml` configuration file:

```xml
<dependency>
    <groupId>com.twitter.heron</groupId>
    <artifactId>heron-dsl</artifactId>
    <version>{{< heronVersion >}}</version>
</dependency>
```

#### Compiling a JAR with dependencies

In order to run a Java DSL topology in a Heron cluster, you'll need to package your topology as a "fat" JAR with dependencies included. You can use the [Maven Assembly Plugin](https://maven.apache.org/plugins/maven-assembly-plugin/usage.html) to generate JARs with dependencies. To install the plugin, add this to your `plugins` block:

```xml
<plugin>
    <artifactId>maven-assembly-plugin</artifactId>
    <version>3.1.0</version>
    <configuration>
        <descriptorRefs>
        <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
    </configuration>
</plugin>
```

```bash
$ mvn assembly:assembly
```

### Gradle installation

```groovy
dependencies {
    compile group: 'com.twitter.heron', name: 'heron-dsl', version: '{{< heronVersion >}}'
}

// Alternatively
dependencies {
    compile 'com.twitter.heron:heron-dsl:{{< heronVersion >}}'
}
```

