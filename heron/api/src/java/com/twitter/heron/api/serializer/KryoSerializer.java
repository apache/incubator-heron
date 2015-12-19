package com.twitter.heron.api.serializer;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer implements IPluggableSerializer {
  private Kryo kryo;
  private Output kryoOut;
  private Input kryoIn;

  public KryoSerializer() { }

  @Override
  public void initialize(Map config) {
    kryo = new Kryo();
    kryo.setReferences(false);
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
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }
}
