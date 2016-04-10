---
date: 2016-02-28T13:10:21-08:00
title: Upgrade Existing Storm Topologies to Heron
---

Heron is designed to be fully backward compatible with existing [Apache Storm](http://storm.apache.org/index.html) projects, allowing simple [Maven POM.xml](https://maven.apache.org/pom.html) changes to migrate existing Storm [topologies](../concepts/topologies).

### Step 1 - Download pre-compiled Heron API binaries with install scripts

Navigate to [Twitter Heron Releases](https://github.com/twitter/heron/releases) and
download the following self extracting binary API install scripts for your platform. 

* heron-api-install

For example, if you want to download for Mac OSX (darwin), the 
corresponding binaries will be

* heron-api-install-\<version\>-darwin.sh

where \<version\> is the desired heron version.

Run the download self installing binary for heron API as follows (example for version 0.13.2). Note "--user --maven" syntax.  
```bash
$ chmod +x heron-api-install-0.13.2-darwin.sh
$ ./heron-api-install-0.13.2-darwin.sh --user --maven
Heron API installer
---------------------

# Binary package at HEAD (@)
   - [Commit](https://github.com/twitter/heron/commit/)
Installing jars to local maven repo.

Heron API is now installed!

See http://heron.github.io/docs/getting-started.html to start a new project!
```

Heron API is now installed in local Maven .m2 repository:
```
~/.m2/repository/com/twitter/heron/heron-api
~/.m2/repository/com/twitter/heron/heron-storm
```

### Step 2 - Add Heron Dependencies to POM.xml file

Copy [POM.xml](https://maven.apache.org/pom.html) dependency segments below and paste into your exsiting Storm POM.xml file in [Dependencies block](https://maven.apache.org/pom.html#Dependencies).
```
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

### Step 3 - Remove Storm Dependencies from POM.xml file
Delete Storm dependency in POM.xml file.
```
    <dependency>
      <groupId>org.apache.storm</groupId>
      <artifactId>storm-core</artifactId>
      <version>storm-VERSION</version>
      <scope>provided</scope>
    </dependency>
```

### Step 4 (if needed) - Remove Clojure Plugin from POM.xml file
Delete Clojure [Plugin](https://maven.apache.org/pom.html#Plugins) from POM.xml file
```
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

### Step 5 - Run Maven Clean, Complie, Package commands
[Run Maven Lifecycle](https://maven.apache.org/run.html) commands
```
$ mvn clean
$ mvn compile
$ mvn package
``` 
Note: [Storm Distribute RPC](http://storm.apache.org/releases/0.10.0/Distributed-RPC.html) is deprecated in Heron.

### Step 5 (optional) - Launch Upgraded Heron Topology
Launch the compiled Maven project on **local cluster** using submit.

Modify your project base directory "{basedir}" and "{PATH-TO-PROJECT}.jar", by [Maven convention](https://maven.apache.org/guides/getting-started/) located in: ${basedir}/target

Modify TOPOLOGY-FILE-NAME and TOPOLOGY-CLASS-NAME to your project.
```bash
$ heron submit local ${basedir}/target/PATH-TO-PROJECT.jar TOPOLOGY-FILE-NAME TOPOLOGY-CLASS-NAME
```

Example submit command using example topology from [Getting Started](../getting-started) guide:

```bash
usage: heron submit local topology-file-name topology-class-name [topology-args]


$ heron submit local ~/.heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology

```

### Next Steps - Deploying or Developing

[Deploying Existing topologies](../operators/deployment/README) in clustered, scheduler-driven environments (Aurora, Mesos, Local)

[Developing Topologies](../concepts/architecture) with the Architecture of Heron