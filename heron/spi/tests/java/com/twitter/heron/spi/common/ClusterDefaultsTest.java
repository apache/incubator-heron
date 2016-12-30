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

package com.twitter.heron.spi.common;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterDefaultsTest {
  private Config props;

  @Before
  public void initialize() {
    props = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaultMiscellaneous())
        .putAll(ClusterDefaults.getDefaultJars())
        .putAll(ClusterDefaults.getSandboxBinaries())
        .build();
  }

  @Test
  public void testSandboxBinaries() throws Exception {

    Assert.assertEquals(
        Defaults.executorSandboxBinary(),
        Context.executorSandboxBinary(props)
    );

    Assert.assertEquals(
        Defaults.stmgrSandboxBinary(),
        Context.stmgrSandboxBinary(props)
    );

    Assert.assertEquals(
        Defaults.tmasterSandboxBinary(),
        Context.tmasterSandboxBinary(props)
    );

    Assert.assertEquals(
        Defaults.shellSandboxBinary(),
        Context.shellSandboxBinary(props)
    );

    Assert.assertEquals(
        Defaults.pythonInstanceSandboxBinary(),
        Context.pythonInstanceSandboxBinary(props)
    );
  }

  @Test
  public void testDefaultJars() throws Exception {
    Assert.assertEquals(
        Defaults.schedulerJar(),
        Context.schedulerJar(props)
    );
  }

  @Test
  public void testDefaultMiscellaneous() throws Exception {
    Assert.assertEquals(
        Defaults.verbose(),
        Context.verbose(props)
    );
    Assert.assertEquals(
        Defaults.schedulerService(),
        Context.schedulerService(props)
    );
  }

  @Test
  public void testDefaultResources() throws Exception {
    Config defaultResources = ClusterDefaults.getDefaultResources();

    Assert.assertEquals(
        Defaults.stmgrRam(),
        Context.stmgrRam(defaultResources)
    );

    Assert.assertEquals(
        Defaults.instanceCpu(),
        Context.instanceCpu(defaultResources),
        0.001
    );

    Assert.assertEquals(
        Defaults.instanceRam(),
        Context.instanceRam(defaultResources)
    );

    Assert.assertEquals(
        Defaults.instanceDisk(),
        Context.instanceDisk(defaultResources)
    );
  }
}
