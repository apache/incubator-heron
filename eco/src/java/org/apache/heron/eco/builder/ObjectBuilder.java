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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.heron.eco.definition.ConfigurationMethodDefinition;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.ObjectDefinition;

public class ObjectBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectBuilder.class);

  private BuilderUtility builderUtility;

  public void setBuilderUtility(BuilderUtility builderUtility) {
    this.builderUtility = builderUtility;
  }

  @SuppressWarnings("rawtypes")
  public Object buildObject(ObjectDefinition def, EcoExecutionContext context)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
      InvocationTargetException, NoSuchFieldException {
    Class clazz = builderUtility.classForName(def.getClassName());

    Object obj;
    if (def.hasConstructorArgs()) {
      LOG.debug("Found constructor arguments in definition ");
      List<Object> cArgs = def.getConstructorArgs();

      if (def.hasReferences()) {
        LOG.debug("The definition has references");
        cArgs = builderUtility.resolveReferences(cArgs, context);
      } else {
        LOG.debug("The definition does not have references");
      }
      LOG.debug("finding compatible constructor for : " + clazz.getName());
      Constructor con = findCompatibleConstructor(cArgs, clazz);
      if (con != null) {
        LOG.debug("Found something seemingly compatible, attempting invocation...");
        obj = con.newInstance(getArgsWithListCoercian(cArgs, con.getParameterTypes()));
      } else {
        String msg = String
            .format("Couldn't find a suitable constructor for class '%s' with arguments '%s'.",
            clazz.getName(),
            cArgs);
        throw new IllegalArgumentException(msg);
      }
    } else {
      obj = clazz.newInstance();
    }
    builderUtility.applyProperties(def, obj, context);
    invokeConfigMethods(def, obj, context);
    return obj;
  }

  @SuppressWarnings("rawtypes")
  protected Constructor findCompatibleConstructor(List<Object> args, Class target) {
    Constructor retval = null;
    int eligibleCount = 0;

    LOG.debug("Target class: " + target.getName() + ", constructor args: " + args);
    Constructor[] cons = target.getDeclaredConstructors();

    for (Constructor con : cons) {
      Class[] paramClasses = con.getParameterTypes();

      if (paramClasses.length == args.size()) {
        LOG.debug("found constructor with same number of args..");
        boolean invokable = canInvokeWithArgs(args, con.getParameterTypes());
        if (invokable) {
          retval = con;
          eligibleCount++;
        }
        LOG.debug("** invokable --> {}" + invokable);
      } else {
        LOG.debug("Skipping constructor with wrong number of arguments.");
      }
    }
    if (eligibleCount > 1) {
      LOG.error("Found multiple invokable constructors for class: "
          + target + ", given arguments " + args + ". Using the last one found.");
    }
    return retval;
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
      LOG.debug("Comparing parameter class " + paramType + " to object class "
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
        LOG.debug("returning false");
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("rawtypes")
  protected boolean isPrimitiveNumber(Class clazz) {
    return clazz.isPrimitive() && !clazz.equals(boolean.class);
  }

  @SuppressWarnings("rawtypes")
  protected boolean isPrimitiveBoolean(Class clazz) {
    return clazz.isPrimitive() && clazz.equals(boolean.class);
  }

  @SuppressWarnings("rawtypes")
  public void invokeConfigMethods(ObjectDefinition bean,
                                         Object instance, EcoExecutionContext context)
      throws InvocationTargetException, IllegalAccessException {

    List<ConfigurationMethodDefinition> methodDefs = bean.getConfigMethods();
    if (methodDefs == null || methodDefs.size() == 0) {
      return;
    }
    Class clazz = instance.getClass();
    for (ConfigurationMethodDefinition methodDef : methodDefs) {
      List<Object> args = methodDef.getArgs();
      if (args == null) {
        args = new ArrayList<Object>();
      }
      if (methodDef.hasReferences()) {
        args = builderUtility.resolveReferences(args, context);
      }
      String methodName = methodDef.getName();
      LOG.debug("method name: " + methodName);
      Method method = findCompatibleMethod(args, clazz, methodName);
      if (method != null) {
        Object[] methodArgs = getArgsWithListCoercian(args, method.getParameterTypes());
        method.invoke(instance, methodArgs);
      } else {
        String msg = String
            .format("Unable to find configuration method '%s' in class '%s' with arguments %s.",
            new Object[]{methodName, clazz.getName(), args});
        throw new IllegalArgumentException(msg);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private Method findCompatibleMethod(List<Object> args, Class target, String methodName) {
    Method retval = null;
    int eligibleCount = 0;
    LOG.debug("Target class: " + target.getName() + ",  methodName: "
        + methodName + ", args: " + args);
    Method[] methods = target.getMethods();
    LOG.debug("methods count: " + methods.length);
    for (Method method : methods) {
      Class[] paramClasses = method.getParameterTypes();
      if (paramClasses.length == args.size() && method.getName().equals(methodName)) {
        LOG.debug("found constructor with same number of args..");
        boolean invokable = false;
        if (args.size() == 0) {
          // it's a method with zero args
          invokable = true;
        } else {
          invokable = canInvokeWithArgs(args, method.getParameterTypes());
        }
        if (invokable) {
          retval = method;
          eligibleCount++;
        }
        LOG.debug("** invokable --> " + invokable);
      } else {
        LOG.debug("Skipping method with wrong number of arguments.");
      }
    }
    if (eligibleCount > 1) {
      LOG.warn("Found multiple invokable methods for class, method, given arguments {} "
          + Arrays.toString(new Object[]{target, methodName, args}));
    }
    return retval;
  }



  /**
   * Given a java.util.List of contructor/method arguments, and a list of parameter types,
   * attempt to convert the
   * list to an java.lang.Object array that can be used to invoke the constructor.
   * If an argument needs
   * to be coerced from a List to an Array, do so.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Object[] getArgsWithListCoercian(List<Object> args, Class[] parameterTypes) {
//        Class[] parameterTypes = constructor.getParameterTypes();
    if (parameterTypes.length != args.size()) {
      throw new IllegalArgumentException("Contructor parameter count does not "
          + "egual argument size.");
    }
    Object[] constructorParams = new Object[args.size()];

    // loop through the arguments, if we hit a list that has to be convered to an array,
    // perform the conversion
    for (int i = 0; i < args.size(); i++) {
      Object obj = args.get(i);
      Class paramType = parameterTypes[i];
      Class objectType = obj.getClass();
      LOG.debug("Comparing parameter class " + paramType.getName() + " to object class "
          +  objectType.getName() + " to see if assignment is possible.");
      if (paramType.equals(objectType)) {
        LOG.debug("They are the same class.");
        constructorParams[i] = args.get(i);
        continue;
      }
      if (paramType.isAssignableFrom(objectType)) {
        LOG.debug("Assignment is possible.");
        constructorParams[i] = args.get(i);
        continue;
      }
      if (isPrimitiveBoolean(paramType) && Boolean.class.isAssignableFrom(objectType)) {
        LOG.debug("Its a primitive boolean.");
        Boolean bool = (Boolean) args.get(i);
        constructorParams[i] = bool.booleanValue();
        continue;
      }
      if (isPrimitiveNumber(paramType) && Number.class.isAssignableFrom(objectType)) {
        LOG.debug("Its a primitive number.");
        Number num = (Number) args.get(i);
        if (paramType == Float.TYPE) {
          constructorParams[i] = num.floatValue();
        } else if (paramType == Double.TYPE) {
          constructorParams[i] = num.doubleValue();
        } else if (paramType == Long.TYPE) {
          constructorParams[i] = num.longValue();
        } else if (paramType == Integer.TYPE) {
          constructorParams[i] = num.intValue();
        } else if (paramType == Short.TYPE) {
          constructorParams[i] = num.shortValue();
        } else if (paramType == Byte.TYPE) {
          constructorParams[i] = num.byteValue();
        } else {
          constructorParams[i] = args.get(i);
        }
        continue;
      }

      // enum conversion
      if (paramType.isEnum() && objectType.equals(String.class)) {
        LOG.debug("Yes, will convert a String to enum");
        constructorParams[i] = Enum.valueOf(paramType, (String) args.get(i));
        continue;
      }

      // List to array conversion
      if (paramType.isArray() && List.class.isAssignableFrom(objectType)) {
        LOG.debug("Conversion appears possible...");
        List list = (List) obj;
        LOG.debug("Array Type: {}, List type: {}" + paramType.getComponentType()
            + list.get(0).getClass());

        // create an array of the right type
        Object newArrayObj = Array.newInstance(paramType.getComponentType(), list.size());
        for (int j = 0; j < list.size(); j++) {
          Array.set(newArrayObj, j, list.get(j));

        }
        constructorParams[i] = newArrayObj;
        LOG.debug("After conversion: {}" + constructorParams[i]);
      }
    }
    return constructorParams;
  }
}
