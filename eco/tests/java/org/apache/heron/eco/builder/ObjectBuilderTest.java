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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.eco.definition.ConfigurationMethodDefinition;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.ObjectDefinition;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.TestWordSpout;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ObjectBuilderTest {

  @Mock
  private ObjectDefinition mockObjectDefinition;
  @Mock
  private EcoExecutionContext mockContext;
  @Mock
  private BuilderUtility mockBuilderUtility;
  @Mock
  private ConfigurationMethodDefinition mockMethodDefinition;
  @InjectMocks
  private ObjectBuilder subject;

  @After
  public void ensureNoUnexpectedMockInteractions() {
    Mockito.verifyNoMoreInteractions(mockContext,
        mockObjectDefinition,
        mockBuilderUtility,
        mockMethodDefinition);
  }

  @Test
  public void buildObject_WithArgsBeanReferenceAndOther_BehavesAsExpected()
      throws ClassNotFoundException,
      InvocationTargetException, NoSuchFieldException,
      InstantiationException, IllegalAccessException {
    final String beanReference1 = "bean1";
    List<Object> constructorArgs = new ArrayList<>();
    List<Object> objects = new ArrayList<>();
    List<Object> firstObject = new ArrayList<>();
    objects.add(firstObject);
    constructorArgs.add(objects);
    Object someComponent = new Object();
    final String className = FixedTuple.class.getName();
    final Class testClass = FixedTuple.class;
    final String methodName = "toString";
    List<ConfigurationMethodDefinition> methodDefinitions = new ArrayList<>();

    methodDefinitions.add(mockMethodDefinition);

    when(mockObjectDefinition.getClassName()).thenReturn(className);
    when(mockObjectDefinition.hasConstructorArgs()).thenReturn(true);
    when(mockObjectDefinition.getConstructorArgs()).thenReturn(constructorArgs);
    when(mockObjectDefinition.hasReferences()).thenReturn(true);
    when(mockContext.getComponent(eq(beanReference1))).thenReturn(someComponent);
    when(mockBuilderUtility.resolveReferences(eq(constructorArgs), eq(mockContext)))
        .thenCallRealMethod();
    when(mockBuilderUtility.classForName(eq(className))).thenReturn(testClass);
    when(mockObjectDefinition.getConfigMethods()).thenReturn(methodDefinitions);
    when(mockMethodDefinition.hasReferences()).thenReturn(true);
    when(mockMethodDefinition.getArgs()).thenReturn(null);
    when(mockMethodDefinition.getName()).thenReturn(methodName);

    Object object = subject.buildObject(mockObjectDefinition, mockContext);

    verify(mockObjectDefinition).getClassName();
    verify(mockObjectDefinition).hasConstructorArgs();
    verify(mockObjectDefinition).getConstructorArgs();
    verify(mockObjectDefinition).hasReferences();
    verify(mockBuilderUtility).classForName(same(className));
    verify(mockBuilderUtility).resolveReferences(same(constructorArgs), same(mockContext));
    verify(mockBuilderUtility).applyProperties(eq(mockObjectDefinition), any(Object.class),
        same(mockContext));
    verify(mockObjectDefinition).getConfigMethods();
    verify(mockMethodDefinition).hasReferences();
    verify(mockMethodDefinition).getArgs();
    verify(mockBuilderUtility, times(2)).resolveReferences(anyListOf(Object.class),
        same(mockContext));
    verify(mockMethodDefinition).getName();

    assertThat(object, is(instanceOf(FixedTuple.class)));
    FixedTuple fixedTuple = (FixedTuple) object;
    assertThat(fixedTuple.values, is(equalTo(objects)));
    assertThat(fixedTuple.values.get(0), is(equalTo(firstObject)));
  }

  @Test
  public void buildObject_NoArgs_BehavesAsExpected()
      throws ClassNotFoundException, InvocationTargetException,
      NoSuchFieldException, InstantiationException, IllegalAccessException {

    final Class fixedTupleClass = TestWordSpout.class;
    final String className = TestWordSpout.class.getName();
    List<ConfigurationMethodDefinition> methodDefinitions = new ArrayList<>();
    final String methodName = "close";

    methodDefinitions.add(mockMethodDefinition);

    when(mockObjectDefinition.getClassName()).thenReturn(className);
    when(mockObjectDefinition.hasConstructorArgs()).thenReturn(false);
    when(mockBuilderUtility.classForName(eq(className))).thenReturn(fixedTupleClass);
    when(mockObjectDefinition.getConfigMethods()).thenReturn(methodDefinitions);
    when(mockMethodDefinition.hasReferences()).thenReturn(false);
    when(mockMethodDefinition.getName()).thenReturn(methodName);

    subject.buildObject(mockObjectDefinition, mockContext);

    verify(mockObjectDefinition).getClassName();
    verify(mockObjectDefinition).hasConstructorArgs();
    verify(mockBuilderUtility).classForName(same(className));
    verify(mockBuilderUtility).applyProperties(same(mockObjectDefinition),
        anyObject(), same(mockContext));
    verify(mockObjectDefinition).getConfigMethods();
    verify(mockMethodDefinition).hasReferences();
    verify(mockMethodDefinition).getName();
    verify(mockMethodDefinition).getArgs();
  }
}

