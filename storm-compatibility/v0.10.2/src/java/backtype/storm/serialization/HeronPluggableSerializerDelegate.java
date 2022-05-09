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

package backtype.storm.serialization;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class HeronPluggableSerializerDelegate implements
    org.apache.heron.api.serializer.IPluggableSerializer {
  private Kryo kryo;
  private Output kryoOut;
  private Input kryoIn;

  public HeronPluggableSerializerDelegate() {
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void initialize(Map config) {
    kryo = SerializationFactory.getKryo(config);
    kryoOut = new Output(2000, 2000000000);
    kryoIn = new Input(1);
  }

  @Override
  public byte[] serialize(Object object) {
    kryoOut.reset();
    kryo.writeClassAndObject(kryoOut, object);
    return kryoOut.toBytes();
  }

  @Override
  public Object deserialize(byte[] input) {
    kryoIn.setBuffer(input);
    return kryo.readClassAndObject(kryoIn);
  }
}
