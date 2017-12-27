//  Copyright 2017 Twitter. All rights reserved.
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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.twitter.heron.eco.definition.BeanListReference;
import com.twitter.heron.eco.definition.BeanReference;
import com.twitter.heron.eco.definition.ConfigurationMethodDefinition;
import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.ObjectDefinition;
import com.twitter.heron.eco.definition.PropertyDefinition;

public abstract class BaseBuilder {
  private static final Logger LOG = Logger.getLogger(BaseBuilder.class.getName());

  @SuppressWarnings("rawtypes")
  protected static Object buildObject(ObjectDefinition def, EcoExecutionContext context)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
      InvocationTargetException, NoSuchFieldException {
    Class clazz = Class.forName(def.getClassName());


    Object obj;
    if (def.hasConstructorArgs()) {
      LOG.fine("Found constructor arguments in definition: "
          + def.getConstructorArgs().getClass().getName());
      List<Object> cArgs = def.getConstructorArgs();
      if (def.hasReferences()) {
        cArgs = resolveReferences(cArgs, context);
      }
      Constructor con = findCompatibleConstructor(cArgs, clazz);
      if (con != null) {
        LOG.fine("Found something seemingly compatible, attempting invocation...");
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
    applyProperties(def, obj, context);
    invokeConfigMethods(def, obj, context);
    return obj;
  }

  @SuppressWarnings("rawtypes")
  public static void invokeConfigMethods(ObjectDefinition bean,
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
        args = resolveReferences(args, context);
      }
      String methodName = methodDef.getName();
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
  private static Method findCompatibleMethod(List<Object> args, Class target, String methodName) {
    Method retval = null;
    int eligibleCount = 0;

    LOG.fine("Target class: " + target.getName() + ",  methodName: "
        + methodName + ", args: " + args);
    Method[] methods = target.getMethods();

    for (Method method : methods) {
      Class[] paramClasses = method.getParameterTypes();
      if (paramClasses.length == args.size() && method.getName().equals(methodName)) {
        LOG.fine("found constructor with same number of args..");
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
        LOG.fine("** invokable --> " + invokable);
      } else {
        LOG.fine("Skipping method with wrong number of arguments.");
      }
    }
    if (eligibleCount > 1) {
      LOG.warning("Found multiple invokable methods for class, method, given arguments {} "
          + new Object[]{target, methodName, args});
    }
    return retval;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static boolean canInvokeWithArgs(List<Object> args, Class[] parameterTypes) {
    if (parameterTypes.length != args.size()) {
      LOG.warning("parameter types were the wrong size");
      return false;
    }

    for (int i = 0; i < args.size(); i++) {
      Object obj = args.get(i);
      if (obj == null) {
        throw new IllegalArgumentException("argument shouldn't be null - index: " + i);
      }
      Class paramType = parameterTypes[i];
      Class objectType = obj.getClass();
      LOG.fine("Comparing parameter class " + paramType + " to object class "
          + objectType + "to see if assignment is possible.");
      if (paramType.equals(objectType)) {
        LOG.fine("Yes, they are the same class.");
      } else if (paramType.isAssignableFrom(objectType)) {
        LOG.fine("Yes, assignment is possible.");
      } else if (isPrimitiveBoolean(paramType) && Boolean.class.isAssignableFrom(objectType)) {
        LOG.fine("Yes, assignment is possible.");
      } else if (isPrimitiveNumber(paramType) && Number.class.isAssignableFrom(objectType)) {
        LOG.fine("Yes, assignment is possible.");
      } else if (paramType.isEnum() && objectType.equals(String.class)) {
        LOG.fine("Yes, will convert a String to enum");
      } else if (paramType.isArray() && List.class.isAssignableFrom(objectType)) {
        // TODO more collection content type checking
        LOG.fine("Assignment is possible if we convert a List to an array.");
        LOG.fine("Array Type: " + paramType.getComponentType() + ", List type: "
            + ((List) obj).get(0).getClass());
      } else {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("rawtypes")
  public static boolean isPrimitiveNumber(Class clazz) {
    return clazz.isPrimitive() && !clazz.equals(boolean.class);
  }

  @SuppressWarnings("rawtypes")
  public static boolean isPrimitiveBoolean(Class clazz) {
    return clazz.isPrimitive() && clazz.equals(boolean.class);
  }

  @SuppressWarnings("rawtypes")
  private static Method findSetter(Class clazz, String property) {
    String setterName = toSetterName(property);
    Method retval = null;
    Method[] methods = clazz.getMethods();
    for (Method method : methods) {
      if (setterName.equals(method.getName())) {
        LOG.fine("Found setter method: " + method.getName());
        retval = method;
      }
    }
    return retval;
  }

  private static String toSetterName(String name) {
    return "set" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
  }

  @SuppressWarnings("rawtypes")
  private static void applyProperties(ObjectDefinition bean, Object instance,
                                      EcoExecutionContext context) throws
      IllegalAccessException, InvocationTargetException, NoSuchFieldException {
    List<PropertyDefinition> props = bean.getProperties();
    Class clazz = instance.getClass();
    if (props != null) {
      for (PropertyDefinition prop : props) {
        Object value = prop.isReference() ? context.getComponent(prop.getRef()) : prop.getValue();
        Method setter = findSetter(clazz, prop.getName());
        if (setter != null) {
          LOG.fine("found setter, attempting to invoke");
          // invoke setter
          setter.invoke(instance, new Object[]{value});
        } else {
          // look for a public instance variable
          LOG.fine("no setter found. Looking for a public instance variable...");
          Field field = findPublicField(clazz, prop.getName());
          if (field != null) {
            field.set(instance, value);
          }
        }
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private static Field findPublicField(Class clazz, String property)
      throws NoSuchFieldException {
    Field field = clazz.getField(property);
    return field;
  }

  @SuppressWarnings("rawtypes")
  private static Constructor findCompatibleConstructor(List<Object> args, Class target) {
    Constructor retval = null;
    int eligibleCount = 0;

    LOG.fine("Target class: " + target.getName() + ", constructor args: " + args);
    Constructor[] cons = target.getDeclaredConstructors();

    for (Constructor con : cons) {
      Class[] paramClasses = con.getParameterTypes();
      if (paramClasses.length == args.size()) {
        LOG.fine("found constructor with same number of args..");
        boolean invokable = canInvokeWithArgs(args, con.getParameterTypes());
        if (invokable) {
          retval = con;
          eligibleCount++;
        }
        LOG.fine("** invokable --> {}" + invokable);
      } else {
        LOG.fine("Skipping constructor with wrong number of arguments.");
      }
    }
    if (eligibleCount > 1) {
      LOG.severe("Found multiple invokable constructors for class: "
          + target + ", given arguments " + args + ". Using the last one found.");
    }
    return retval;
  }

  @SuppressWarnings("rawtypes")
  private static List<Object> resolveReferences(List<Object> args, EcoExecutionContext context) {
    LOG.fine("Checking arguments for references.");
    List<Object> cArgs = new ArrayList<Object>();
    // resolve references
    for (Object arg : args) {
      if (arg instanceof BeanReference) {
        cArgs.add(context.getComponent(((BeanReference) arg).getId()));
      } else if (arg instanceof BeanListReference) {
        List<Object> components = new ArrayList<>();
        BeanListReference ref = (BeanListReference) arg;
        for (String id : ref.getIds()) {
          components.add(context.getComponent(id));
        }

        LOG.fine("BeanListReference resolved as {}" + components);
        cArgs.add(components);
      } else {
        cArgs.add(arg);
      }
    }
    return cArgs;
  }

  /**
   * Given a java.util.List of contructor/method arguments, and a list of parameter types,
   * attempt to convert the
   * list to an java.lang.Object array that can be used to invoke the constructor.
   * If an argument needs
   * to be coerced from a List to an Array, do so.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Object[] getArgsWithListCoercian(List<Object> args, Class[] parameterTypes) {
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
      LOG.fine("Comparing parameter class " + paramType.getName() + " to object class "
          +  objectType.getName() + " to see if assignment is possible.");
      if (paramType.equals(objectType)) {
        LOG.fine("They are the same class.");
        constructorParams[i] = args.get(i);
        continue;
      }
      if (paramType.isAssignableFrom(objectType)) {
        LOG.fine("Assignment is possible.");
        constructorParams[i] = args.get(i);
        continue;
      }
      if (isPrimitiveBoolean(paramType) && Boolean.class.isAssignableFrom(objectType)) {
        LOG.fine("Its a primitive boolean.");
        Boolean bool = (Boolean) args.get(i);
        constructorParams[i] = bool.booleanValue();
        continue;
      }
      if (isPrimitiveNumber(paramType) && Number.class.isAssignableFrom(objectType)) {
        LOG.fine("Its a primitive number.");
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
        LOG.fine("Yes, will convert a String to enum");
        constructorParams[i] = Enum.valueOf(paramType, (String) args.get(i));
        continue;
      }

      // List to array conversion
      if (paramType.isArray() && List.class.isAssignableFrom(objectType)) {
        // TODO more collection content type checking
        LOG.fine("Conversion appears possible...");
        List list = (List) obj;
        LOG.fine("Array Type: {}, List type: {}" + paramType.getComponentType()
            + list.get(0).getClass());

        // create an array of the right type
        Object newArrayObj = Array.newInstance(paramType.getComponentType(), list.size());
        for (int j = 0; j < list.size(); j++) {
          Array.set(newArrayObj, j, list.get(j));

        }
        constructorParams[i] = newArrayObj;
        LOG.fine("After conversion: {}" + constructorParams[i]);
      }
    }
    return constructorParams;
  }
}
