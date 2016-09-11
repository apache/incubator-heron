---
title: Upgrade Storm Topologies to Heron
---

Heron is designed to be fully backward compatible with existing [Apache
Storm](http://storm.apache.org/index.html) projects, which means that you can
migrate an existing Storm [topology](../concepts/topologies) to Heron by making
just a few adjustments to the topology's `pom.xml` [Maven configuration
file](https://maven.apache.org/pom.html).

## Step 1 --- Download Heron API binaries with an installation script

Go to the [releases](https://github.com/twitter/heron/releases) page for Heron
and download the appropriate installation script for your platform. The name of
the script has this form:

* `heron-api-install-{{% heronVersion %}}-PLATFORM.sh`

The script for Mac OS X (`darwin`), for example, would be named
`heron-api-install-{{% heronVersion %}}-darwin.sh`.

Once the script is downloaded, run it while setting the `--user` and
`--maven` flags:

```bash
$ chmod +x heron-api-install-{{% heronVersion %}}-PLATFORM.sh
$ ./heron-api-install-{{% heronVersion %}}-PLATFORM.sh --user --maven
Heron API installer
-------------------

Installing jars to local maven repo.
tar xfz /var/folders/8r/x6dwcnkn4p9_rgwvq_3jg6y00000gn/T/heron.XXXX.EnJDpZNb/heron-api.tar.gz
-C /var/folders/8r/x6dwcnkn4p9_rgwvq_3jg6y00000gn/T/heron.XXXX.EnJDpZNb

Heron API is now installed!

See http://heronstreaming.io/docs/getting-started for how to use Heron.

heron.build.version : '{{% heronVersion %}}'
heron.build.time : ...
heron.build.timestamp : ...
heron.build.host : ${HOSTNAME}
heron.build.user : ${USERNAME}
heron.build.git.revision : ...
heron.build.git.status : Clean
```

The Heron API will now be installed in your local [Maven
repository](https://maven.apache.org/settings.html):

```bash
$ ls ~/.m2/repository/com/twitter/heron
heron-api
heron-spi
heron-storm
```

## Step 2 --- Add Heron dependencies to  `pom.xml`

Copy the [`pom.xml`](https://maven.apache.org/pom.html) segments below and paste
them into your existing Storm `pom.xml` file in the [dependencies
block](https://maven.apache.org/pom.html#Dependencies).

```xml
<dependency>
  <groupId>com.twitter.heron</groupId>
  <artifactId>heron-api</artifactId>
  <version>SNAPSHOT</version>
  <scope>compile</scope>
</dependency>
<dependency>
  <groupId>com.twitter.heron</groupId>
  <artifactId>heron-storm</artifactId>
  <version>SNAPSHOT</version>
  <scope>compile</scope>
</dependency>
```

## Step 3 --- Remove Storm dependencies from `pom.xml`

Delete the Storm dependency, which looks like this:

```xml
<dependency>
  <groupId>org.apache.storm</groupId>
  <artifactId>storm-core</artifactId>
  <version>storm-VERSION</version>
  <scope>provided</scope>
</dependency>
```

## Step 4 (if needed) --- Remove the Clojure plugin from `pom.xml`

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

## Step 5 --- Run Maven commands

Run the following [Maven lifecycle](https://maven.apache.org/run.html) commands:

```bash
$ mvn clean
$ mvn compile
$ mvn package
```

**Note**: [Storm Distribute
RPC](http://storm.apache.org/releases/0.10.0/Distributed-RPC.html) is deprecated
in Heron.

## Step 5 (optional) --- Launch your upgraded Heron topology

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
  com.twitter.heron.examples.ExclamationTopology \ # The topology's Java class
  ExclamationTopology # The name of the topology
```

### Next Steps

* [Deploy topologies](../operators/deployment) in clustered, scheduler-driven
  environments (such as on [Aurora](../operators/deployment/schedulers/aurora)
  and
  [locally](../operators/deployment/schedulers/local))
* [Develop topologies](../concepts/architecture) for Heron
