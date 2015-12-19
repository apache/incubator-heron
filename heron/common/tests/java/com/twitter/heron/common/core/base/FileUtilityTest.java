package com.twitter.heron.common.core.base;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import junit.framework.Assert;

/**
 * FileUtility Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileUtility.class})
public class FileUtilityTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: deleteFile(String filename)
   */
  @Test
  public void testDeleteFile() throws Exception {
    PowerMockito.mockStatic(Files.class);

    Assert.assertTrue(FileUtility.deleteFile(""));
  }

  /**
   * Method: copyFile(String source, String target)
   */
  @Test
  public void testCopyFile() throws Exception {
    PowerMockito.mockStatic(Files.class);

    Assert.assertTrue(FileUtility.copyFile("", ""));
  }

  /**
   * Method: writeToFile(String filename, byte[] contents)
   */
  @Test
  public void testWriteToFile() throws Exception {
    String currentWorkingDir = Paths.get("").toAbsolutePath().normalize().toString();
    Assert.assertFalse(FileUtility.writeToFile(currentWorkingDir, null));

    PowerMockito.mockStatic(Files.class);
    String randomString = UUID.randomUUID().toString();
    Assert.assertTrue(FileUtility.writeToFile(randomString, null));
  }

  /**
   * Method: readFromFile(String filename)
   */
  @Test
  public void testReadFromFile() throws Exception {
    String toRead = "abc";
    PowerMockito.mockStatic(Files.class);
    PowerMockito.when(Files.readAllBytes(Matchers.any(Path.class))).thenReturn(toRead.getBytes());

    Assert.assertEquals(new String(FileUtility.readFromFile("")), toRead);
  }

  /**
   * Method: createDirectory(String directory)
   */
  @Test
  public void testCreateDirectory() throws Exception {
    String currentWorkingDir = Paths.get("").toAbsolutePath().normalize().toString();
    Assert.assertFalse(FileUtility.createDirectory(currentWorkingDir));

    String newDir = String.format("%s/%s", currentWorkingDir, UUID.randomUUID().toString());
    Assert.assertTrue(FileUtility.createDirectory(newDir));

    Assert.assertTrue(FileUtility.deleteFile(newDir));
  }

  /**
   * Method: isDirectoryExists(String directory)
   */
  @Test
  public void testIsDirectoryExists() throws Exception {
    PowerMockito.mockStatic(Files.class);
    PowerMockito.when(Files.isDirectory(Matchers.any(Path.class))).thenReturn(true);

    Assert.assertTrue(FileUtility.isDirectoryExists(""));

    PowerMockito.when(Files.isDirectory(Matchers.any(Path.class))).thenReturn(false);

    Assert.assertFalse(FileUtility.isDirectoryExists(""));
  }

  /**
   * Method: isOriginalPackageJar(String packageFilename)
   */
  @Test
  public void testIsOriginalPackageJar() throws Exception {
    String jarFile = "a.jar";
    Assert.assertTrue(FileUtility.isOriginalPackageJar(jarFile));

    String notJarFile = "b.tar";
    Assert.assertFalse(FileUtility.isOriginalPackageJar(notJarFile));
  }

  /**
   * Method: getBaseName(String file)
   */
  @Test
  public void testGetBaseName() throws Exception {
    String filename = "a/b";
    Assert.assertEquals("b", FileUtility.getBaseName(filename));

    filename = "b";
    Assert.assertEquals("b", FileUtility.getBaseName(filename));
  }
}
