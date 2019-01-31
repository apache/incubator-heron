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

package org.apache.heron.simulator;

import java.lang.reflect.Field;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;

/**
 * Simulator Tester
 */
public class SimulatorTest {

  @SuppressWarnings("unchecked")
  private static void clearSingletonRegistry() throws Exception {
    // Remove the Singleton by Reflection
    Field field = SingletonRegistry.INSTANCE.getClass().getDeclaredField("singletonObjects");
    field.setAccessible(true);

    Map<String, Object> singletonObjects =
        (Map<String, Object>) field.get(SingletonRegistry.INSTANCE);
    singletonObjects.clear();
  }

  /**
   * Method: Init()
   */
  @Test
  public void testInit() throws Exception {
    clearSingletonRegistry();
    Simulator spySimulator = Mockito.spy(new Simulator(false));

    spySimulator.init();
    Mockito.verify(spySimulator, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(
        spySimulator, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));

    spySimulator.init();
    Mockito.verify(spySimulator, Mockito.times(2)).isSystemConfigExisted();
    Mockito.verify(
        spySimulator, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));
  }

  @Test
  public void testTwoLocaMode() throws Exception {
    clearSingletonRegistry();

    Simulator spySimulator1 = Mockito.spy(new Simulator(false));
    spySimulator1.init();
    Mockito.verify(spySimulator1, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(
        spySimulator1, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));

    Simulator spySimulator2 = Mockito.spy(new Simulator(false));
    spySimulator2.init();
    Mockito.verify(spySimulator2, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(
        spySimulator2, Mockito.times(0)).registerSystemConfig(Mockito.any(SystemConfig.class));
  }
}
