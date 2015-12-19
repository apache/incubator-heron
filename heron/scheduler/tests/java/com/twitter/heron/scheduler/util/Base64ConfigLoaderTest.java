package com.twitter.heron.scheduler.util;

import java.nio.charset.Charset;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.junit.Assert;
import org.junit.Test;

public class Base64ConfigLoaderTest {
  @Test
  public void testBase64DecodedOverride() throws Exception {
    Base64ConfigLoader loader = Base64ConfigLoader.class.newInstance();
    loader.properties = new Properties();
    loader.applyConfigOverride(DatatypeConverter.printBase64Binary(
        "key=value".getBytes(Charset.forName("UTF-8"))));
    Assert.assertEquals("value", loader.properties.getProperty("key"));
  }
}
