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

import org.apache.storm.testing.TestWordSpout;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;


public class BuilderUtilityTest {

  private BuilderUtility subject;

  @Before
  public void setUpForEachTestCase() {
    subject = new BuilderUtility();
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

    Class clazz = subject.classForName(TestWordSpout.class.getName());

    assertThat(clazz, notNullValue());
  }

}