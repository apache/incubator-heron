// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.integration_test.topology.serialization;

import java.io.Serializable;

/**
 * An example Java-serializable custom object.
 */
public class CustomObject implements Serializable {

  private static final long serialVersionUID = -7502839214902002412L;
  private String name;
  private Integer age;

  public CustomObject(String name, Integer age) {
    this.name = name;
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public Integer getAge() {
    return age;
  }

  @Override
  public String toString() {
    return "CustomObject{" + "name='" + name + '\'' + ", age=" + age + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CustomObject that = (CustomObject) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    return age != null ? age.equals(that.age) : that.age == null;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (age != null ? age.hashCode() : 0);
    return result;
  }
}
