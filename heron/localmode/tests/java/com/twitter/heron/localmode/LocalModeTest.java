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

package com.twitter.heron.localmode;

import java.lang.reflect.Field;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;

/**
 * LocalMode Tester
 */
public class LocalModeTest {

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
    LocalMode spyLocalMode = Mockito.spy(new LocalMode(false));

    spyLocalMode.init();
    Mockito.verify(spyLocalMode, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(spyLocalMode, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));

    spyLocalMode.init();
    Mockito.verify(spyLocalMode, Mockito.times(2)).isSystemConfigExisted();
    Mockito.verify(spyLocalMode, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));
  }

  @Test
  public void testTwoLocaMode() throws Exception {
    clearSingletonRegistry();

    LocalMode spyLocalMode1 = Mockito.spy(new LocalMode(false));
    spyLocalMode1.init();
    Mockito.verify(spyLocalMode1, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(spyLocalMode1, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));

    LocalMode spyLocalMode2 = Mockito.spy(new LocalMode(false));
    spyLocalMode2.init();
    Mockito.verify(spyLocalMode2, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(spyLocalMode2, Mockito.times(0)).registerSystemConfig(Mockito.any(SystemConfig.class));
  }
}
