package com.twitter.heron.common.core.base;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum SingletonRegistry {
  INSTANCE;

  /**
   * Cache of singleton objects: bean name --> instance
   */
  private final Map<String, Object> singletonObjects = new ConcurrentHashMap<String, Object>();

  public boolean containsSingleton(String beanName) {
    return singletonObjects.containsKey(beanName);
  }

  public Object getSingleton(String beanName) {
    return this.singletonObjects.get(beanName);
  }

  public int getSingletonCount() {
    return singletonObjects.size();
  }

  // Typically invoked during registry configuration, but can also be used for runtime registration
  // of singletons. As a consequence, a registry implementation should synchronize singleton access.
  public void registerSingleton(String beanName, Object singletonObject) {
    assert beanName != null && singletonObject != null;

    synchronized (this.singletonObjects) {
      Object oldObject = this.singletonObjects.get(beanName);
      if (oldObject != null) {
        throw new IllegalStateException("Could not register object [" + singletonObject +
            "] under bean name '" + beanName + "': there is already object [" + oldObject + "] bound");
      }
      this.singletonObjects.put(beanName, singletonObject);
    }
  }

  // Typically invoked during registry configuration, but can also be used for runtime registration
  // of singletons. As a consequence, a registry implementation should synchronize singleton access.
  // Singleton has to be registered first or otherwise IllegalStateException would throw
  public void updateSingleton(String beanName, Object singletonObject) {
    assert beanName != null && singletonObject != null;

    synchronized (this.singletonObjects) {
      if (!this.singletonObjects.containsKey(beanName)) {
        throw new IllegalStateException("Could not update object [" + singletonObject +
            "] under bean name '" + beanName + "': it have not been registered yet.");

      }
      this.singletonObjects.put(beanName, singletonObject);
    }
  }
}
