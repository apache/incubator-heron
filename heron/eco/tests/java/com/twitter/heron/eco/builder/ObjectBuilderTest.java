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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.twitter.heron.eco.definition.BeanListReference;
import com.twitter.heron.eco.definition.BeanReference;
import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.ObjectDefinition;

import static org.hamcrest.CoreMatchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ObjectBuilderTest {

  @Mock
  private ObjectDefinition mockObjectDefinition;
  @Mock
  private EcoExecutionContext mockContext;

  private ObjectBuilder subject;

  @Before
  public void setUpForEachTestCase() {
    subject = new ObjectBuilder();
  }

  @After
  public void ensureNoUnexpectedMockInteractions() {
    Mockito.verifyNoMoreInteractions(mockContext,
        mockObjectDefinition);
  }

  @Test
  @Ignore //TODO: This class is too much to test.  Need to refactor a bit
  public void buildObject_WithArgs_BeanReferenceAndOther() throws ClassNotFoundException, InvocationTargetException, NoSuchFieldException, InstantiationException, IllegalAccessException {
    final String beanReference1 = "bean1";
    List<Object> constructorArgs = new ArrayList<>();
    BeanReference beanReference = new BeanReference(beanReference1);
    constructorArgs.add(beanReference);
    constructorArgs.add("otherArg");
    Object someComponent = new Object();
    when(mockObjectDefinition.getClassName()).thenReturn("com.twitter.heron.eco.builder.TestClass");
    when(mockObjectDefinition.hasConstructorArgs()).thenReturn(true);
    when(mockObjectDefinition.getConstructorArgs()).thenReturn(constructorArgs);
    when(mockObjectDefinition.hasReferences()).thenReturn(true);
    when(mockContext.getComponent(eq(beanReference1))).thenReturn(someComponent);

    subject.buildObject(mockObjectDefinition, mockContext);

    verify(mockObjectDefinition).getClassName();
    verify(mockObjectDefinition).hasConstructorArgs();
    verify(mockObjectDefinition).getConstructorArgs();
    verify(mockObjectDefinition).hasReferences();
    verify(mockContext).getComponent(same(beanReference1));

  }

  public class TestClass {

    public TestClass(String str1, String str2){

    }
  }

}