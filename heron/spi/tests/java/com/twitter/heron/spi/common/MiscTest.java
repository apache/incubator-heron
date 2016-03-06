package com.twitter.heron.spi.common;

import junit.framework.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MiscTest {

  private static final Logger LOG = Logger.getLogger(MiscTest.class.getName());

  @Test
  public void testHeronHome() {
    // check no occurrence 
    Assert.assertEquals(
        "./bin",
        Misc.substitute("/usr/local/heron", "./bin")
    );

    // check a single subsitution at the begining
    Assert.assertEquals(
        "/usr/local/heron/bin",
        Misc.substitute("/usr/local/heron", "${HERON_HOME}/bin")
    );

    // check a single subsitution at the begining with relative path
    Assert.assertEquals(
        "./usr/local/heron/bin",
        Misc.substitute("/usr/local/heron", "./${HERON_HOME}/bin")
    );

    // check a single substitution at the end
    Assert.assertEquals(
        "/bin/usr/local/heron",
        Misc.substitute("/usr/local/heron", "/bin/${HERON_HOME}")
    );

    // check a single substitution at the end with relative path
    Assert.assertEquals(
        "./bin/usr/local/heron",
        Misc.substitute("/usr/local/heron", "./bin/${HERON_HOME}")
    );

    // check a single substitution in the middle
    Assert.assertEquals(
        "/bin/usr/local/heron/etc",
        Misc.substitute("/usr/local/heron", "/bin/${HERON_HOME}/etc")
    );

    // check a single substitution in the middle with relative path
    Assert.assertEquals(
        "./bin/usr/local/heron/etc",
        Misc.substitute("/usr/local/heron", "./bin/${HERON_HOME}/etc")
    );
  }

  @Test
  public void testURL() {
    Assert.assertTrue(
        Misc.isURL("file:///users/john/afile.txt")
    );
    Assert.assertFalse(
        Misc.isURL("/users/john/afile.txt")
    );
    Assert.assertTrue(
        Misc.isURL("https://gotoanywebsite.net/afile.html")
    );
    Assert.assertFalse(
        Misc.isURL("https//gotoanywebsite.net//afile.html")
    );
  }
}
