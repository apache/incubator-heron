/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.spi.common;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ContextTest {
  private Config props;

  @Before
  public void initialize() {
    props = Config.newBuilder(true).build();
  }

  @Test
  public void testBinaries() throws Exception {

    assertEquals(Key.EXECUTOR_BINARY.getDefault(), Context.executorBinary(props));
    assertEquals(Key.STMGR_BINARY.getDefault(), Context.stmgrBinary(props));
    assertEquals(Key.TMANAGER_BINARY.getDefault(), Context.tmanagerBinary(props));
    assertEquals(Key.SHELL_BINARY.getDefault(), Context.shellBinary(props));
    assertEquals(
        Key.PYTHON_INSTANCE_BINARY.getDefault(),
        Context.pythonInstanceBinary(props)
    );
  }

  @Test
  public void testDefaultJars() throws Exception {
    assertEquals(Key.SCHEDULER_JAR.getDefault(), Context.schedulerJar(props));
  }

  @Test
  public void testDefaultMiscellaneous() throws Exception {
    assertEquals(Key.VERBOSE.getDefault(), Context.verbose(props));
    assertEquals(Key.SCHEDULER_IS_SERVICE.getDefault(), Context.schedulerService(props));
  }

  @Test
  public void testDefaultResources() throws Exception {
    Config defaultResources = props;

    assertEquals(Key.STMGR_RAM.getDefault(), Context.stmgrRam(defaultResources));
    assertEquals(Key.CKPTMGR_RAM.getDefault(), Context.ckptmgrRam(defaultResources));
    assertEquals(Key.METRICSMGR_RAM.getDefault(), Context.metricsmgrRam(defaultResources));
    assertEquals(
        (Double) Key.INSTANCE_CPU.getDefault(), Context.instanceCpu(defaultResources), 0.001);
    assertEquals(Key.INSTANCE_RAM.getDefault(), Context.instanceRam(defaultResources));
    assertEquals(Key.INSTANCE_DISK.getDefault(), Context.instanceDisk(defaultResources));
  }
}
