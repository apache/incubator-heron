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
