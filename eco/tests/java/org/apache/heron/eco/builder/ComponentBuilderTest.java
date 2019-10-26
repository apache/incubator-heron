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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.eco.definition.BeanDefinition;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.EcoTopologyDefinition;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ComponentBuilderTest {

  @Mock
  private EcoExecutionContext mockContext;
  @Mock
  private ObjectBuilder mockObjectBuilder;

  private ComponentBuilder subject;

  @Before
  public void setUpForEachTestCase() {
    subject = new ComponentBuilder();
  }

  @After
  public void ensureNoUnexpectedMockInteractions() {
    Mockito.verifyNoMoreInteractions(mockContext,
        mockObjectBuilder);
  }

  @Test
  public void testBuildComponents_AllGood_BehavesAsExpected() throws ClassNotFoundException,
      InvocationTargetException, NoSuchFieldException,
      InstantiationException, IllegalAccessException {
    BeanDefinition beanDefinition = new BeanDefinition();
    final String id = "bean";
    beanDefinition.setId(id);
    BeanDefinition beanDefinition1 = new BeanDefinition();
    final String id1 = "bean1";
    beanDefinition1.setId(id1);
    List<BeanDefinition> componentDefinitions = new ArrayList<>();
    componentDefinitions.add(beanDefinition);
    componentDefinitions.add(beanDefinition1);
    EcoTopologyDefinition ecoTopologyDefinition = new EcoTopologyDefinition();
    ecoTopologyDefinition.setComponents(componentDefinitions);
    Object object = new Object();
    Object object1 = new Object();

    when(mockContext.getTopologyDefinition()).thenReturn(ecoTopologyDefinition);
    when(mockObjectBuilder.buildObject(eq(beanDefinition), eq(mockContext))).thenReturn(object);
    when(mockObjectBuilder.buildObject(eq(beanDefinition1), eq(mockContext))).thenReturn(object1);

    subject.buildComponents(mockContext, mockObjectBuilder);

    verify(mockContext).getTopologyDefinition();
    verify(mockObjectBuilder).buildObject(same(beanDefinition), same(mockContext));
    verify(mockObjectBuilder).buildObject(same(beanDefinition1), same(mockContext));
    verify(mockContext).addComponent(eq(id), anyObject());
    verify(mockContext).addComponent(eq(id1), anyObject());
  }
}
