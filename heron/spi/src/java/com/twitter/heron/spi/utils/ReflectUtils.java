package com.twitter.heron.spi.util;

public class ReflectUtils {
  public static <T> T createInstance(String className) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    return (T) Class.forName(className).newInstance(); 
  }
}
