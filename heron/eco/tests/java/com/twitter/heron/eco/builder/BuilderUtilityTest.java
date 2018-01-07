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

import org.apache.storm.testing.TestWordSpout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.twitter.heron.eco.definition.BeanReference;
import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.ObjectDefinition;
import com.twitter.heron.eco.definition.PropertyDefinition;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BuilderUtilityTest {

  @Mock
  private ObjectDefinition mockObjectDefinition;
  @Mock
  private Object mockObject;
  @Mock
  private EcoExecutionContext mockContext;

  private BuilderUtility subject;

  @Before
  public void setUpForEachTestCase() {
    subject = new BuilderUtility();
  }

  @After
  public void ensureNoUnexpectedMockInteractions() {
    Mockito.verifyNoMoreInteractions(mockObject,
        mockObjectDefinition,
        mockContext);
  }

  @Test
  public void toSetterName_ReturnsCorrectName() {
    final String name = "name";
    final String expectedName = "setName";
    String setterName = subject.toSetterName(name);

    assertThat(setterName, is(equalTo(expectedName)));
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void classForName_ReturnsCorrectClass() throws ClassNotFoundException {
    final String className = TestWordSpout.class.getName();

    Class clazz = subject.classForName(className);

    assertThat(clazz, notNullValue());
    assertThat(className, is(equalTo(clazz.getName())));
  }

  @Test
  public void applyProperties_SetterFound_BehavesAsExpected()
      throws IllegalAccessException, NoSuchFieldException,
      InvocationTargetException {
    final String id = "id";
    final String ref = "ref";
    String fakeComponent = "component";
    BeanReference beanReference = new BeanReference(id);
    List<PropertyDefinition> propertyDefinitions = new ArrayList<>();
    PropertyDefinition propertyDefinition = new PropertyDefinition();
    propertyDefinition.setRef(ref);
    propertyDefinition.setName(id);
    propertyDefinitions.add(propertyDefinition);

    when(mockObjectDefinition.getProperties()).thenReturn(propertyDefinitions);
    when(mockContext.getComponent(eq(ref))).thenReturn(fakeComponent);

    subject.applyProperties(mockObjectDefinition, beanReference, mockContext);

    verify(mockContext).getComponent(same(ref));
    verify(mockObjectDefinition).getProperties();
  }

}