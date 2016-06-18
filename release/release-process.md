---
title: Release Process
---
<-- TODO -->
# How to Update Version

<-- TODO -->
# How to Tag

<-- TODO -->
# Test Process

<-- TODO -->
# Build Process


# Publish to Maven Central

## Account Setup
Following [sonatype.org](http://central.sonatype.org/)

1. Request access to publish to com.twitter by creating account and requesting access in https://issues.sonatype.org
2. Twitter contact needed to confirm access [see example](https://issues.sonatype.org/browse/OSSRH-22297)
3. Install local GPG/PGP to sign files by following [sonatype instructions](http://central.sonatype.org/pages/requirements.html#sign-files-with-gpgpgp) 
4. Publish public key [see example](http://central.sonatype.org/pages/working-with-pgp-signatures.html#distributing-your-public-key)
5. Confirm public key is published [see example](http://pgp.mit.edu/) note leading ```0x``` required for hex keys)

# Release Steps
Follow [Manual Staging Bundle Creation and Deployment](http://central.sonatype.org/pages/manual-staging-bundle-creation-and-deployment.html) by signing files manually, steps summarized below.

Three artifacts are required for each version: ```heron-api```, ```heron-storm```, and ```heron-spi```
Example POM template files required located in ```heron/release/maven-central/```

<-- TODO refer to release "build process" instructions, not download  -->
<-- TODO update for "heron-spi" artifact when available -->
### Step 1 - Download and install ```heron-api-install-{VERSION}-{DIST}.sh``` from https://github.com/twitter/heron/releases

Example for version ```0.14.0``` installed using ```--user```
```
$ wget https://github.com/twitter/heron/releases/download/0.14.0/heron-api-install-0.14.0-darwin.sh
$ chmod +x heron-api-install-0.14.0-darwin.sh
$ ./heron-api-install-0.14.0-darwin.sh --user
```
Jars are installed into ```~/.heronapi```

### Step 2 - Create temp folder for each ```heron-api``` and ```heron-storm``` artifact 
Example, create folder named ```heron-api-bundle-0.14.0```

Artifact bundle must contain:

1. {artifact-version}.jar
2. {artifact-version}.pom
3. {artifact-version}-javadoc.jar
4. {artifact-version}-sources.jar


Note "heron-api-0.14.0-javadoc.jar" and "heron-api-0.14.0-sources.jar" are currently empty due to Bazel build but required by Maven Central checks.  

Copy artifact {artifact}.jar from ```~/.heronapi``` to temp folder and rename to {artifact-version}.jar

Generate versioned POM file using ```./maven-central/maven-pom-version.sh VERSION``` 

```
$ ./maven-central/maven-pom-version.sh 0.14.1
$ ls
heron-api-0.14.1.pom
heron-storm-0.14.1.pom
heron-spi-0.14.1.pom
...
```
Copy versioned POM files to artifact temp directory

### Step 3 - Manually sign each file using GPG/PGP 
Use GPG/PGP to sign each file by following [steps](http://central.sonatype.org/pages/requirements.html#sign-files-with-gpgpgp)

Example on MacOSX using gpg 
```
$ gpg -ab heron-api-0.14.0-javadoc.jar
$ ls 
heron-api-0.14.0-javadoc.jar
heron-api-0.14.0-javadoc.jar.asc
```

### Step 3 - Create ```bundle.jar``` for each ```heron-api``` and ```heron-storm``` artifact
For each artifact temp file, for example ```heron-api-bundle-0.14.0``` following .jar and signed .asc files exist:
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

Create ```bundle.jar```
```
$ jar -cvf bundle.jar ./
$ ls 
bundle.jar
...
```

### Step 4 - Upload each ```bundle.jar``` sonatype.org
Login to https://oss.sonatype.org/#welcome

For each artifact, ```heron-api``` and ```heron-storm```, separately upload the ```bundle.jar```:

From the Staging Upload tab, select Artifact Bundle from the Upload Mode dropdown.

Then click the Select Bundle to Upload button, and select the bundle you just created.

Click the Upload Bundle button. If the upload is successful, a staging repository will be created, and you can proceed with [releasing](http://central.sonatype.org/pages/releasing-the-deployment.html).

Note, since ```com.twitter.heron``` already exists, once released, artifacts will immediately be synced with Maven Central.  

Check Maven Central ```com.twitter.heron``` and ```version``` exists http://search.maven.org/

Check local heron version topology compiles by deleting local ```~/.heronapi```, updating project POM file to version, and pulling from Maven Central.  

