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

package com.twitter.heron.serializer.kryo

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer implements IPluggableSerializer {
  private Kryo kryo;
  private Output kryoOut;
  private Input kryoIn;

  public KryoSerializer() {
  }

  @Override
  public void initialize(Map<String, Object> config) {
    kryo = new Kryo();
    kryo.setReferences(false);
    kryoOut = new Output(2000, 2000000000);
    kryoIn = new Input(1);
  }

  @Override
  public byte[] serialize(Object object) {
    kryoOut.clear();
    kryo.writeClassAndObject(kryoOut, object);
    return kryoOut.toBytes();
  }

  @Override
  public Object deserialize(byte[] input) {
    kryoIn.setBuffer(input);
    return kryo.readClassAndObject(kryoIn);
  }
}
