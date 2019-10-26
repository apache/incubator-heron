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

package org.apache.heron.eco.builder;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.heron.eco.definition.BeanListReference;
import org.apache.heron.eco.definition.BeanReference;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.ObjectDefinition;
import org.apache.heron.eco.definition.PropertyDefinition;

public class BuilderUtility {

  private static final Logger LOG = LoggerFactory.getLogger(BuilderUtility.class);

  @SuppressWarnings("rawtypes")
  protected List<Object> resolveReferences(List<Object> args, EcoExecutionContext context) {
    LOG.debug("Checking arguments for references.");
    List<Object> cArgs = new ArrayList<>();

    // resolve references
    for (Object arg : args) {
      if (arg instanceof BeanReference) {
        LOG.debug("BeanReference: " + ((BeanReference) arg).getId());
        cArgs.add(context.getComponent(((BeanReference) arg).getId()));
      } else if (arg instanceof BeanListReference) {
        List<Object> components = new ArrayList<>();
        BeanListReference ref = (BeanListReference) arg;
        for (String id : ref.getIds()) {
          components.add(context.getComponent(id));
        }

        LOG.debug("BeanListReference resolved as {}" + components);
        cArgs.add(components);
      } else {
        LOG.debug("Unknown:" + arg.toString());
        cArgs.add(arg);
      }
    }
    return cArgs;
  }

  @SuppressWarnings("rawtypes")
  protected void applyProperties(ObjectDefinition bean, Object instance,
                                      EcoExecutionContext context) throws
      IllegalAccessException, InvocationTargetException, NoSuchFieldException {
    List<PropertyDefinition> props = bean.getProperties();
    Class clazz = instance.getClass();
    if (props != null) {
      for (PropertyDefinition prop : props) {
        Object value = prop.isReference() ? context.getComponent(prop.getRef()) : prop.getValue();
        Method setter = findSetter(clazz, prop.getName());
        if (setter != null) {
          LOG.debug("found setter, attempting with: " + instance.getClass() + "  " + value);
          // invoke setter
          setter.invoke(instance, new Object[]{value});
        } else {
          // look for a public instance variable
          LOG.debug("no setter found. Looking for a public instance variable...");
          Field field = findPublicField(clazz, prop.getName());
          if (field != null) {
            field.set(instance, value);
          }
        }
      }
    }
  }

  @SuppressWarnings("rawtypes")
  protected Field findPublicField(Class clazz, String property)
      throws NoSuchFieldException {
    Field field = clazz.getField(property);
    return field;
  }

  @SuppressWarnings("rawtypes")
  private Method findSetter(Class clazz, String property) {
    String setterName = toSetterName(property);
    Method retval = null;
    Method[] methods = clazz.getMethods();
    for (Method method : methods) {
      if (setterName.equals(method.getName())) {
        LOG.debug("Found setter method: " + method.getName());
        retval = method;
      }
    }
    return retval;
  }

  protected String toSetterName(String name) {
    return "set" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
  }

  protected Class<?> classForName(String className) throws ClassNotFoundException {
    return Class.forName(className);
  }
}
