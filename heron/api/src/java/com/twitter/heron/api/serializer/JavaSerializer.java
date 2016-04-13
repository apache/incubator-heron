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

package com.twitter.heron.api.serializer;

import java.util.Map;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JavaSerializer implements IPluggableSerializer {

  @Override
  public void initialize(Map config) { }

  @Override
  public byte[] serialize(Object object) { 
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(object);
      oos.flush();
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
    return bos.toByteArray();
  }
    
  @Override
  public Object deserialize(byte[] _input) {
    ByteArrayInputStream bis = new ByteArrayInputStream(_input);
    try {
      ObjectInputStream ois = new ObjectInputStream(bis);
      return ois.readObject();
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }
}
