---
title: Migrate Storm Topologies to Heron
---

Heron is designed to be fully backwards compatible with existing [Apache
Storm](http://storm.apache.org/index.html) projects, which means that you can
migrate an existing Storm [topology](../concepts/topologies) to Heron by making
just a few adjustments to the topology's `pom.xml` [Maven configuration
file](https://maven.apache.org/pom.html).

## Step 1. Add Heron dependencies to  `pom.xml`

Copy the [`pom.xml`](https://maven.apache.org/pom.html) segments below and paste
them into your existing Storm `pom.xml` file in the [dependencies
block](https://maven.apache.org/pom.html#Dependencies).

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

## Step 2. Remove Storm dependencies from `pom.xml`

Delete the Storm dependency, which looks like this:

```xml
<dependency>
  <groupId>org.apache.storm</groupId>
  <artifactId>storm-core</artifactId>
  <version>storm-VERSION</version>
  <scope>provided</scope>
</dependency>
```

## Step 3 (if needed). Remove the Clojure plugin from `pom.xml`

Delete the [Clojure plugin](https://maven.apache.org/pom.html#Plugins), which
should look like this:

```xml
<plugin>
  <groupId>com.theoryinpractise</groupId>
  <artifactId>clojure-maven-plugin</artifactId>
  <version>1.3.12</version>
  <extensions>true</extensions>
  <configuration>
    <sourceDirectories>
      <sourceDirectory>src/clj</sourceDirectory>
    </sourceDirectories>
  </configuration>
</plugin>
```

## Step 4. Run Maven commands

Run the following [Maven lifecycle](https://maven.apache.org/run.html) commands:

```bash
$ mvn clean
$ mvn compile
$ mvn package
```

> [Storm Distribute RPC](http://storm.apache.org/releases/0.10.0/Distributed-RPC.html) is deprecated in Heron.

## Step 4 (optional). Launch your upgraded Heron topology

You can launch the compiled Maven project on your [local
cluster](../operators/deployment/schedulers/local) using `heron submit`.

First, modify your project's base directory `{basedir}` and
`{PATH-TO-PROJECT}.jar`, which is located in `${basedir}/target` by [Maven
convention](https://maven.apache.org/guides/getting-started/). Then modify the
`TOPOLOGY-FILE-NAME` and `TOPOLOGY-CLASS-NAME` for your project:

```bash
$ heron submit local \
  ${basedir}/target/PATH-TO-PROJECT.jar \
  TOPOLOGY-FILE-NAME \
  TOPOLOGY-CLASS-NAME
```

Here's an example submit command using the example topology from the [Quick
Start Guide](../getting-started) guide:

```bash
$ heron submit local \
  ~/.heron/examples/heron-examples.jar \ # The path of the topology's jar file
  org.apache.heron.examples.ExclamationTopology \ # The topology's Java class
  ExclamationTopology # The name of the topology
```

### Next Steps

* [Deploy topologies](../operators/deployment) in clustered, scheduler-driven
  environments (such as on [Aurora](../operators/deployment/schedulers/aurora)
  and
  [locally](../operators/deployment/schedulers/local))
* [Develop topologies](../concepts/architecture) for Heron
