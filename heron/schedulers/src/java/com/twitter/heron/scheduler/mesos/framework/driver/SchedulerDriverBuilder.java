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

package com.twitter.heron.scheduler.mesos.framework.driver;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;

public final class SchedulerDriverBuilder {
  private static final Logger LOG = Logger.getLogger(SchedulerDriverBuilder.class.getName());

  private Protos.Credential credential;
  private Protos.FrameworkInfo frameworkInfo;
  private FrameworkConfiguration config;
  private Scheduler scheduler;

  private SchedulerDriverBuilder() {
  }

  public static SchedulerDriverBuilder newBuilder() {
    return new SchedulerDriverBuilder();
  }

  public SchedulerDriverBuilder setScheduler(Scheduler sched) {
    this.scheduler = sched;
    return this;
  }

  public SchedulerDriverBuilder setCredentials(String principal, String secretFile) {
    if (principal == null || secretFile == null) {
      credential = null;
      return this;
    }

    Protos.Credential.Builder credentialBuilder =
        Protos.Credential.newBuilder().setPrincipal(principal);

    try {
      ByteString secretBytes = ByteString.readFrom(new FileInputStream(secretFile));

      Set<PosixFilePermission> filePermissions =
          Files.getPosixFilePermissions(Paths.get(secretFile));

      if (filePermissions.contains(PosixFilePermission.OTHERS_READ)
          || filePermissions.contains(PosixFilePermission.OTHERS_WRITE)) {
        LOG.warning("Secret file $secretFile should not be globally accessible.");
      }

      credentialBuilder.setSecret(secretBytes);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error reading authentication secret from file: " + secretFile, e);
    }

    credential = credentialBuilder.build();
    return this;
  }

  public SchedulerDriverBuilder setFrameworkInfo(FrameworkConfiguration frameworkConfig,
                                                 Protos.FrameworkID frameworkId) {
    this.config = frameworkConfig;
    Protos.FrameworkInfo.Builder frameworkInfoBuilder = Protos.FrameworkInfo.newBuilder()
        .setName(frameworkConfig.schedulerName)
        .setCheckpoint(frameworkConfig.checkpoint)
        .setRole(frameworkConfig.role)
        .setFailoverTimeout(frameworkConfig.failoverTimeoutSeconds)
        .setUser(frameworkConfig.user)
        .setHostname(frameworkConfig.hostname);

    // Set the ID, if provided
    if (frameworkId != null) {
      frameworkInfoBuilder.setId(frameworkId);
    }

    // set the authentication principal, if provided
    if (frameworkConfig.authenticationPrincipal != null) {
      frameworkInfoBuilder.setPrincipal(frameworkConfig.authenticationPrincipal);
    }

    frameworkInfo = frameworkInfoBuilder.build();
    return this;
  }

  public SchedulerDriver build() {
    if (frameworkInfo == null || scheduler == null) {
      throw new IllegalArgumentException("FrameworkInfo or Scheduler is not set");
    }

    return credential == null
        ? new MesosSchedulerDriver(scheduler, frameworkInfo, config.master)
        : new MesosSchedulerDriver(scheduler, frameworkInfo, config.master, credential);
  }
}
