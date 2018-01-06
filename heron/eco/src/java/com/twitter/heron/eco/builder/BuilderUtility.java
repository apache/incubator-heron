//  Copyright 2018 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.eco.builder;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.eco.definition.BeanListReference;
import com.twitter.heron.eco.definition.BeanReference;
import com.twitter.heron.eco.definition.EcoExecutionContext;

public class BuilderUtility {

  private static final Logger LOG = LoggerFactory.getLogger(BuilderUtility.class);

  @SuppressWarnings("rawtypes")
  protected List<Object> resolveReferences(List<Object> args, EcoExecutionContext context) {
    LOG.debug("Checking arguments for references.");
    List<Object> cArgs = new ArrayList<>();
    // resolve references
    for (Object arg : args) {
      if (arg instanceof BeanReference) {
        LOG.info("BeanReference: " + ((BeanReference) arg).getId());
        cArgs.add(context.getComponent(((BeanReference) arg).getId()));
      } else if (arg instanceof BeanListReference) {
        List<Object> components = new ArrayList<>();
        BeanListReference ref = (BeanListReference) arg;
        for (String id : ref.getIds()) {
          components.add(context.getComponent(id));
        }

        LOG.info("BeanListReference resolved as {}" + components);
        cArgs.add(components);
      } else {
        LOG.info("Unknown:" + arg.toString());
        cArgs.add(arg);
      }
    }
    return cArgs;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected boolean canInvokeWithArgs(List<Object> args, Class[] parameterTypes) {
    if (parameterTypes.length != args.size()) {
      LOG.warn("parameter types were the wrong size");
      return false;
    }

    for (int i = 0; i < args.size(); i++) {
      Object obj = args.get(i);
      if (obj == null) {
        throw new IllegalArgumentException("argument shouldn't be null - index: " + i);
      }
      Class paramType = parameterTypes[i];
      Class objectType = obj.getClass();
      LOG.info("Comparing parameter class " + paramType + " to object class "
          + objectType + "to see if assignment is possible.");
      if (paramType.equals(objectType)) {
        LOG.debug("Yes, they are the same class.");
      } else if (paramType.isAssignableFrom(objectType)) {
        LOG.debug("Yes, assignment is possible.");
      } else if (isPrimitiveBoolean(paramType) && Boolean.class.isAssignableFrom(objectType)) {
        LOG.debug("Yes, assignment is possible.");
      } else if (isPrimitiveNumber(paramType) && Number.class.isAssignableFrom(objectType)) {
        LOG.debug("Yes, assignment is possible.");
      } else if (paramType.isEnum() && objectType.equals(String.class)) {
        LOG.debug("Yes, will convert a String to enum");
      } else if (paramType.isArray() && List.class.isAssignableFrom(objectType)) {
        LOG.debug("Assignment is possible if we convert a List to an array.");
        LOG.debug("Array Type: " + paramType.getComponentType() + ", List type: "
            + ((List) obj).get(0).getClass());
      } else {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("rawtypes")
  public boolean isPrimitiveNumber(Class clazz) {
    return clazz.isPrimitive() && !clazz.equals(boolean.class);
  }

  @SuppressWarnings("rawtypes")
  public boolean isPrimitiveBoolean(Class clazz) {
    return clazz.isPrimitive() && clazz.equals(boolean.class);
  }
}
