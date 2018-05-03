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

package org.apache.heron.common.basics;

import java.util.HashMap;
import java.util.Map;

public enum SingletonRegistry {
  INSTANCE;

  /**
   * Cache of singleton objects: bean name --> instance
   * We use HashMap and synchronized all the operations on it.
   * The reasons are:
   * 1. The register methods need to be atomic, so make these methods synchronized.
   * 2. We need synchronized to guarantee the thread safe of singletonObjects (HashMap itself
   * is not thread safe).
   * 3. ConcurrentHashMap doesn't support the register atomic operations and thus we didn't use
   * it here
   */
  private final Map<String, Object> singletonObjects = new HashMap<String, Object>();

  public boolean containsSingleton(String beanName) {
    synchronized (this.singletonObjects) {
      return singletonObjects.containsKey(beanName);
    }
  }

  public Object getSingleton(String beanName) {
    synchronized (this.singletonObjects) {
      return this.singletonObjects.get(beanName);
    }
  }

  public int getSingletonCount() {
    synchronized (this.singletonObjects) {
      return singletonObjects.size();
    }
  }

  // Typically invoked during registry configuration, but can also be used for runtime registration
  // of singletons. As a consequence, a registry implementation should synchronize singleton access.
  public void registerSingleton(String beanName, Object singletonObject) {
    assert beanName != null && singletonObject != null;

    synchronized (this.singletonObjects) {
      Object oldObject = this.singletonObjects.get(beanName);
      if (oldObject != null) {
        throw new IllegalStateException("Could not register object [" + singletonObject
            + "] under bean name '" + beanName + "': there is already object [" + oldObject
            + "] bound");
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
        throw new IllegalStateException("Could not update object [" + singletonObject
            + "] under bean name '" + beanName + "': it have not been registered yet.");

      }
      this.singletonObjects.put(beanName, singletonObject);
    }
  }
}
