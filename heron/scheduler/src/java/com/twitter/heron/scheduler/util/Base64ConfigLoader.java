package com.twitter.heron.scheduler.util;

import java.nio.charset.Charset;

import javax.xml.bind.DatatypeConverter;

// Converts the config String argument from Base64 Binary back into the original String in UTF-8,
// and then invoke applyConfigOverride(..) with the decoded String.

public class Base64ConfigLoader extends DefaultConfigLoader {
  @Override
  protected String preparePropertyOverride(String configOverride) {
    return new String(DatatypeConverter.parseBase64Binary(configOverride), Charset.forName("UTF-8"));
  }
}
