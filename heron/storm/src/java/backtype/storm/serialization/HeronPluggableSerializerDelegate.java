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

package backtype.storm.serialization;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class HeronPluggableSerializerDelegate implements com.twitter.heron.api.serializer.IPluggableSerializer {
    private Kryo kryo;
    private Output kryoOut;
    private Input kryoIn;

    public HeronPluggableSerializerDelegate() {
    }

    @Override
    public void initialize(Map config) {
        kryo = SerializationFactory.getKryo(config);
        kryoOut = new Output(2000, 2000000000);
        kryoIn = new Input(1);
    }

    @Override
    public byte[] serialize(Object object) {
        try {
            kryoOut.clear();
            kryo.writeClassAndObject(kryoOut, object);
            return kryoOut.toBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object deserialize(byte[] input) {
        try {
            kryoIn.setBuffer(input);
            return kryo.readClassAndObject(kryoIn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
