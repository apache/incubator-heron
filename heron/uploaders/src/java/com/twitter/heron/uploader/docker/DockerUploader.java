// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.uploader.docker;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.uploader.IUploader;

/**
 * IUploader implementation that creates a Docker image for the Topology
 * <p>
 * The created Docker image will be based on the given base image and include the topology definition
 * and binary in a fixed location. The uploader optionally pushes the docker image to a remote registry.
 * Downstream schedulers must take responsibility for leveraging this image, by either building an additional
 * Docker image based on it with the correct CMD directive or otherwise starting the topology.
 * </p>
 * <p>
 * The topology definition will be written to "/home/[role]/[topology-name]"
 * if no role is given "default" is used
 * The URI returned by uploadPackage is the fully qualified tag name it will be
 * "[docker-registry]/[cluster]/[role]/[env]/[topology-name]:[UUID]"
 * if role or env are not specified then "default" is used, if docker-registry is not
 * specified then it is omitted.
 * UUIDs are used for the version in order to ensure that previously cached images won't get used.
 * Per docker requirements role, env, and topology-name are all snake cased with Capital letters
 * made lowercase and proceeded by a "-" unless the first character, for instance TopologyName would
 * become topology-name
 * </p>
 * <p>
 * The following configuration parameters are allowed:
 * <dl>
 * <dt>heron.uploader.docker.base</dt>
 * <dd>The base docker image to use in the FROM directive, must be specified</dd>
 * <dt>heron.uploader.docker.registry</dt>
 * <dd>The registry prefix to used with the image tag, may be omitted</dd>
 * <dt>heron.uploader.docker.push</dt>
 * <dd>Boolean value if the docker image should be pushed to the remote registry, defaults to false</dd>
 * </dl>
 * </p>
 * <p>
 * The Dockerfile generated will be as follows:
 * <code>
 * FROM [base-image]
 * ADD [topology.tar]   /home/[role | "heron"]/[topology-name]
 * </code>
 * Assuming your topology was named "MyTopology" and based on the ubuntu:trusty image with the role
 * "MyRole" and packed into a tarfile called "MyTopology-3424.tar" the result would be.
 * <code>
 * FROM ubuntu:trusty
 * ADD MyTopology-3424.tar  /home/MyRole/MyTopology
 * </code>
 * </p>
 */
public final class DockerUploader implements IUploader {

  private static final Logger LOG = Logger.getLogger(DockerUploader.class.getName());

  private Config configuration;
  private final Dockerfile dockerfile;
  private final DockerDaemon dockerDaemon;

  public DockerUploader() {
    this(new Dockerfile(), new DockerDaemon());
  }

  public DockerUploader(Dockerfile dockerfile, DockerDaemon dockerDaemon) {
    this.dockerfile = dockerfile;
    this.dockerDaemon = dockerDaemon;
  }

  @Override
  public void initialize(Config config) {
    this.configuration = config;
    LOG.info("Initializing DockerUploader Uploader");
  }

  @Override
  public URI uploadPackage() {
    // get the topology package file, role, and name
    File topologyPackageLocation =
        new File(Context.topologyPackageFile(configuration));
    String role = Context.role(configuration);
    String topologyName = Context.topologyName(configuration);

    // check that the base image is defined
    String baseContainer = DockerContext.baseImage(configuration);
    if (baseContainer == null || baseContainer.isEmpty()) {
      LOG.log(Level.SEVERE, "Unable to create Dockerfile without base image specified.");
      return null;
    }

    //write a temp Dockerfile
    File workingDir = topologyPackageLocation.getParentFile();
    LOG.info("Creating DockerUploader file in " + workingDir.getAbsolutePath());
    try {
      dockerfile.newDockerfile(workingDir)
          .FROM(baseContainer)
          .ADD(topologyPackageLocation.getName(), "/home/" + role + "/" + topologyName)
          .write();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error Writing DockerUploader File", e);
      return null;
    }

    // build the tag name
    final StringBuilder tagNameBuilder = new StringBuilder();
    String registry = DockerContext.dockerRegistry(configuration);
    if (registry != null && !registry.isEmpty()) {
      tagNameBuilder.append(registry).append("/");
    }

    String tagName = tagNameBuilder
        .append(toSnakeCase(Context.cluster(configuration))).append("/")
        .append(toSnakeCase(role)).append("/")
        .append(toSnakeCase(Context.environ(configuration))).append("/")
        .append(toSnakeCase(topologyName)).append(":")
        .append(UUID.randomUUID().toString())
        .toString();

    // try to build the Dockerfile
    if (!dockerDaemon.build(workingDir, tagName)) {
      return null;
    }

    // if pushing try to push
    if (DockerContext.push(configuration)) {
      if (!dockerDaemon.push(tagName)) {
        return null;
      }
    }

    // return a URI with the tag name
    return URI.create(tagName);
  }

  // return the camelCase string transformed to snake-case
  private String toSnakeCase(String camelCase) {
    StringBuilder builder = new StringBuilder(camelCase.length());
    boolean isFirst = true;
    for (char c : camelCase.toCharArray()) {
      if (Character.isUpperCase(c)) {
        if (!isFirst) {
          builder.append('-');
        }
        builder.append(Character.toLowerCase(c));
      } else {
        builder.append(c);
      }
      isFirst = false;
    }
    return builder.toString();
  }

  @Override
  public boolean undo() {
    return false;
  }

  @Override
  public void close() {
  }

}
