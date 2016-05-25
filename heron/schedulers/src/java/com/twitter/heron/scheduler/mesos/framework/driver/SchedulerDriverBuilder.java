package com.twitter.heron.scheduler.mesos.framework.driver;

import com.google.protobuf.ByteString;
import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SchedulerDriverBuilder {
  private static final Logger LOG = Logger.getLogger(SchedulerDriverBuilder.class.getName());

  Protos.Credential credential;
  Protos.FrameworkInfo frameworkInfo;
  FrameworkConfiguration config;
  Scheduler scheduler;

  private SchedulerDriverBuilder() {
  }

  public static SchedulerDriverBuilder newBuilder() {
    return new SchedulerDriverBuilder();
  }

  public SchedulerDriverBuilder setScheduler(Scheduler scheduler) {
    this.scheduler = scheduler;
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

      if (filePermissions.contains(PosixFilePermission.OTHERS_READ) ||
          filePermissions.contains(PosixFilePermission.OTHERS_WRITE)) {
        LOG.warning("Secret file $secretFile should not be globally accessible.");
      }

      credentialBuilder.setSecret(secretBytes);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error reading authentication secret from file: " + secretFile, e);
    }

    credential = credentialBuilder.build();
    return this;
  }

  public SchedulerDriverBuilder setFrameworkInfo(FrameworkConfiguration config,
                                                 Protos.FrameworkID frameworkId) {
    this.config = config;
    Protos.FrameworkInfo.Builder frameworkInfoBuilder = Protos.FrameworkInfo.newBuilder()
        .setName(config.schedulerName)
        .setCheckpoint(config.checkpoint)
        .setRole(config.role)
        .setFailoverTimeout(config.failoverTimeoutSeconds)
        .setUser(config.user)
        .setHostname(config.hostname);

    // Set the ID, if provided
    if (frameworkId != null) {
      frameworkInfoBuilder.setId(frameworkId);
    }

    // set the authentication principal, if provided
    if (config.authenticationPrincipal != null) {
      frameworkInfoBuilder.setPrincipal(config.authenticationPrincipal);
    }

    frameworkInfo = frameworkInfoBuilder.build();
    return this;
  }

  public SchedulerDriver build() {
    if (frameworkInfo == null || scheduler == null) {
      throw new IllegalArgumentException("FrameworkInfo or Scheduler is not set");
    }

    return credential == null ?
        new MesosSchedulerDriver(scheduler, frameworkInfo, config.master) :
        new MesosSchedulerDriver(scheduler, frameworkInfo, config.master, credential);
  }
}
