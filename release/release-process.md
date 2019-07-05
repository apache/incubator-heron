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
---
title: Release Process
---
<!-- TODO -->
# How to Update Version

<!-- TODO -->
# How to Tag

<!-- TODO -->
# Test Process

<!-- TODO -->
# Build Process


# Publish to Maven Central

## Account Setup

Steps taken from Maven Release process outlined by [sonatype.org](http://central.sonatype.org/) and summarized below.

1. Request access to publish to org.apache by creating account and requesting access in [sonatype.org](https://issues.sonatype.org).
2. Twitter contact needed to confirm access [(see example)](https://issues.sonatype.org/browse/OSSRH-22297).
3. Install local GPG/PGP to sign files by following [sonatype instructions](http://central.sonatype.org/pages/requirements.html#sign-files-with-gpgpgp). 
4. Publish public key [(see example)](http://central.sonatype.org/pages/working-with-pgp-signatures.html#distributing-your-public-key).
5. Confirm public key is published [(see example)](http://pgp.mit.edu/). Note leading `0x` required for hex keys.

# Release Steps

Follow [Manual Staging Bundle Creation and Deployment](http://central.sonatype.org/pages/manual-staging-bundle-creation-and-deployment.html) by signing files manually, see steps summarized below.

Three artifacts are required for each version: `heron-api`, `heron-storm`, and `heron-spi`.
Example POM template files required located in `heron/release/maven/`.

<!-- TODO refer to release "build process" instructions, not download  -->
<!-- TODO update for "heron-spi" artifact when available -->
### Step 1 - Download and install `heron-api-install-{VERSION}-{DIST}.sh` from [github releases](https://github.com/apache/incubator-heron/releases)

Example command for `heron-api` version `0.14.0` installed using the `--user` flag:
```
$ wget https://github.com/apache/incubator-heron/releases/download/0.14.0/heron-api-install-0.14.0-darwin.sh
$ chmod +x heron-api-install-0.14.0-darwin.sh
$ ./heron-api-install-0.14.0-darwin.sh --user
```
After running the above install script with `--user` flag, the `heron-api` artifacts are installed into the `~/.heronapi` directory.

### Step 2 - Create temp folder for each `heron-api`, `heron-storm` and `heron-spi` artifact 

Example, create folder named `heron-api-bundle-0.14.0`.

Artifact bundle must contain the following with Maven Central naming format:

1. `{artifact-name}-{artifact-version}.jar`
2. `{artifact-name}-{artifact-version}.pom`
3. `{artifact-name}-{artifact-version}-javadoc.jar`
4. `{artifact-name}-{artifact-version}-sources.jar`


Note `heron-api-0.14.0-javadoc.jar` and `heron-api-0.14.0-sources.jar` are currently placeholders due to Bazel version but required by Maven Central checks.  

Copy artifact `{artifact}.jar` from `~/.heronapi` to temp folder and rename to `{artifact-name}-{artifact-version}.jar`.

Generate versioned POM file using `./maven/maven-pom-version.sh VERSION`.
Example:

```
$ ./maven/maven-pom-version.sh 0.14.1
$ ls
heron-api-0.14.1.pom
heron-storm-0.14.1.pom
heron-spi-0.14.1.pom
...
```
Copy versioned POM files to artifact temp directory.

### Step 3 - Manually sign each file using GPG/PGP 

Use GPG/PGP to sign each file by following [steps](http://central.sonatype.org/pages/requirements.html#sign-files-with-gpgpgp).

Example on Mac OS X using gpg:
```
$ gpg -ab heron-api-0.14.0-javadoc.jar
$ ls 
heron-api-0.14.0-javadoc.jar
heron-api-0.14.0-javadoc.jar.asc
```

### Step 4 - Create `bundle.jar` for each `heron-api`, `heron-storm` and `heron-spi` artifact

For each artifact temp file, for example `heron-api-bundle-0.14.0` following .jar and signed .asc files are required:
```
$ cd heron-api-bundle-0.14.0
$ ls 
heron-api-0.14.0-sources.jar  
heron-api-0.14.0-sources.jar.asc   
heron-api-0.14.0-javadoc.jar     
heron-api-0.14.0-javadoc.jar.asc 
heron-api-0.14.0.jar   
heron-api-0.14.0.jar.asc
heron-api-0.14.0.pom          
heron-api-0.14.0.pom.asc
```

Create `bundle.jar` of temp directory contents.
```
$ jar -cvf bundle.jar ./
$ ls 
bundle.jar
...
```

### Step 5 - Upload each `bundle.jar` to [sonatype.org](https://oss.sonatype.org/#welcome) and Release to Maven Central

Login to [sonatype.org](https://oss.sonatype.org/#welcome).

For each artifact, `heron-api`, `heron-storm` and `heron-spi`, separately upload the `bundle.jar` created in Step 3 with the following steps:

1. From the `Staging Upload` tab, select `Artifact Bundle` from the `Upload Mode` dropdown.

2. Click the **Select Bundle to Upload** button, and select the `bundle.jar` you created.

3. Click the **Upload Bundle** button. If the upload is successful, a staging repository will be created, and you can proceed with [releasing steps](http://central.sonatype.org/pages/releasing-the-deployment.html), summarized below:
  1.  After you deployment the repository will be in an `Open` status.
  2.  You can evaluate the deployed components in the repository using the `Contents` tab.
  3.  If you believe everything is correct you, can press the **Close** button above the list.
  4.  This will trigger the evaluations of the components against the requirements.
  5.  `Closing` will fail if your components do not meet the requirements. If this happens, you can press **Drop** and the staging repository will be deleted.
  6.  Once you have successfully closed the staging repository, you can release it by pressing the **Release** button.

Note, since `org.apache.heron` already exists, once released, artifacts will immediately be synced with Maven Central.

4. Check that artifacts are successfully released by going to [Maven's search page](http://search.maven.org/) and searching for `org.apache.heron` with proper `version`.

5. As a final end-to-end check, ensure that you can successfully pull the correct `version` artifacts from Maven Central in a local topology. To do this:
  1. Delete your local `~/.heronapi` directory.
  2. Run `mvn clean` in your local heron topology project directory.
  3. Update the project `POM.xml` file with new upgraded heron version, [see upgrade-storm-to-heron example - Step 2](http://twitter.github.io/heron/docs/upgrade-storm-to-heron/).
  4. Run `mvn compile` to confirm your project can successfully pull updated version artifacts from Maven Central.

