package com.twitter.heron.localmode;

import java.lang.reflect.Field;
import java.util.Map;

import org.junit.Test;

import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * LocalMode Tester
 */
public class LocalModeTest {
  /**
   * Method: Init()
   */
  @Test
  public void testInit() throws Exception {
    clearSingletonRegistry();
    LocalMode spyLocalMode = spy(new LocalMode(false));

    spyLocalMode.init();
    verify(spyLocalMode, times(1)).isSystemConfigExisted();
    verify(spyLocalMode, times(1)).registerSystemConfig(any(SystemConfig.class));

    spyLocalMode.init();
    verify(spyLocalMode, times(2)).isSystemConfigExisted();
    verify(spyLocalMode, times(1)).registerSystemConfig(any(SystemConfig.class));
  }

  @Test
  public void testTwoLocaMode() throws Exception {
    clearSingletonRegistry();
    try {
      LocalMode spyLocalMode1 = spy(new LocalMode(false));
      spyLocalMode1.init();
      verify(spyLocalMode1, times(1)).isSystemConfigExisted();
      verify(spyLocalMode1, times(1)).registerSystemConfig(any(SystemConfig.class));

      LocalMode spyLocalMode2 = spy(new LocalMode(false));
      spyLocalMode2.init();
      verify(spyLocalMode2, times(1)).isSystemConfigExisted();
      verify(spyLocalMode2, times(0)).registerSystemConfig(any(SystemConfig.class));
    } catch (Exception e) {
      fail(String.format("Exception %s thrown while creating two LocalMode", e));
    }
  }

  private static void clearSingletonRegistry() throws Exception {
    // Remove the Singleton by Reflection
    Field field = SingletonRegistry.INSTANCE.getClass().getDeclaredField("singletonObjects");
    field.setAccessible(true);
    Map<String, Object> singletonObjects = (Map<String, Object>) field.get(SingletonRegistry.INSTANCE);
    singletonObjects.clear();
  }
}
