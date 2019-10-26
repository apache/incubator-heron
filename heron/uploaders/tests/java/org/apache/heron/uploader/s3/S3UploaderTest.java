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

package org.apache.heron.uploader.s3;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Map;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.uploader.UploaderException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3UploaderTest {
  private S3Uploader uploader;
  private AmazonS3 mockS3Client;
  private Config.Builder configBuilder;
  private File tempFile;

  @Before
  public void setUp() throws Exception {
    mockS3Client = mock(AmazonS3.class);

    tempFile = File.createTempFile("temp-file-name", ".tmp");
    tempFile.deleteOnExit();

    configBuilder = Config.newBuilder()
        .put(S3Context.HERON_UPLOADER_S3_BUCKET, "bucket")
        .put(Key.TOPOLOGY_NAME, "test-topology")
        .put(Key.TOPOLOGY_PACKAGE_FILE, "topology.tar.gz");

    uploader = new S3Uploader();
    uploader.initialize(configBuilder.build());
    uploader.s3Client = mockS3Client;
  }

  @Test
  public void uploadTopologyToS3CompatibleStorage() throws Exception {
    Map<String, String> env = System.getenv();

    String s3Url = env.get("S3_URL");
    String accessKey = env.get("S3_ACCESS_KEY");
    String secretKey = env.get("S3_SECRET_KEY");

    if (s3Url == null || accessKey == null || secretKey == null) {
      // skip test no detail provided
      return;
    }

    Config.Builder s3Config = Config.newBuilder()
        .put(S3Context.HERON_UPLOADER_S3_BUCKET, "testbucket")
        .put(S3Context.HERON_UPLOADER_S3_ACCESS_KEY, accessKey)
        .put(S3Context.HERON_UPLOADER_S3_SECRET_KEY, secretKey)
        .put(S3Context.HERON_UPLOADER_S3_URI, s3Url)
        .put(Key.TOPOLOGY_NAME, "test-topology")
        .put(Key.TOPOLOGY_PACKAGE_FILE, tempFile.getAbsolutePath());

    S3Uploader s3Uploader = new S3Uploader();
    s3Uploader.initialize(s3Config.build());

    String expectedUri = s3Url + "testbucket/test-topology/" + tempFile.getName();
    assertEquals(new URI(expectedUri), s3Uploader.uploadPackage());
  }

  @Test
  public void uploadTopologyToS3() throws Exception {
    String expectedRemotePath = "test-topology/topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.doesObjectExist(expectedBucket, expectedRemotePath)).thenReturn(false);
    when(mockS3Client.getUrl(expectedBucket, expectedRemotePath)).thenReturn(new URL("http://url"));

    URI uri = uploader.uploadPackage();

    verify(mockS3Client).putObject(Mockito.eq(expectedBucket),
        Mockito.eq(expectedRemotePath), Mockito.any(File.class));

    verify(mockS3Client).getUrl(expectedBucket, expectedRemotePath);

    assertEquals(new URI("http://url"), uri);
  }

  @Test
  public void backupPreviousVersionOnDeployForRollback() throws Exception {
    String expectedRemotePath = "test-topology/topology.tar.gz";
    String expectedPreviousVersionPath = "test-topology/previous_topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.doesObjectExist(expectedBucket, expectedRemotePath)).thenReturn(true);
    when(mockS3Client.getUrl(expectedBucket, expectedRemotePath)).thenReturn(new URL("http://url"));

    URI uri = uploader.uploadPackage();

    verify(mockS3Client).copyObject(expectedBucket, expectedRemotePath, expectedBucket,
        expectedPreviousVersionPath);

    verify(mockS3Client).putObject(Mockito.eq(expectedBucket), Mockito.eq(expectedRemotePath),
        Mockito.any(File.class));

    verify(mockS3Client).getUrl(expectedBucket, expectedRemotePath);

    assertEquals(new URI("http://url"), uri);
  }

  @Test(expected = UploaderException.class)
  @SuppressWarnings("unchecked")
  public void handlePutObjectExceptionOnUpload() throws Exception {
    String expectedRemotePath = "test-topology/topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.doesObjectExist(expectedBucket, expectedRemotePath)).thenReturn(true);
    when(mockS3Client.putObject(Mockito.eq(expectedBucket), Mockito.eq(expectedRemotePath),
        Mockito.any(File.class))).thenThrow(SdkClientException.class);
    uploader.uploadPackage();
  }

  @Test
  public void restoreThePreviousVersionOnUndo() {
    String expectedRemotePath = "test-topology/topology.tar.gz";
    String expectedPreviousVersionPath = "test-topology/previous_topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.doesObjectExist(
        expectedBucket, expectedPreviousVersionPath)).thenReturn(true);

    uploader.undo();

    verify(mockS3Client).copyObject(expectedBucket, expectedPreviousVersionPath,
        expectedBucket, expectedRemotePath);
  }

  @Test
  public void doNotRestorePreviousVersionIfItDoesNotExist() {
    String expectedPreviousVersionPath = "test-topology/previous_topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.doesObjectExist(expectedBucket, expectedPreviousVersionPath))
        .thenReturn(false);

    uploader.undo();

    verify(mockS3Client, never()).copyObject(Mockito.anyString(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void cleanUpPreviousVersionOnClose() {
    String expectedPreviousVersionPath = "test-topology/previous_topology.tar.gz";
    String expectedBucket = "bucket";

    uploader.close();
    verify(mockS3Client).deleteObject(expectedBucket, expectedPreviousVersionPath);
  }


  @Test
  public void PrefixUploadPathWithSpecifiedPrefix() throws Exception {
    configBuilder.put(S3Context.HERON_UPLOADER_S3_PATH_PREFIX, "path/prefix");
    uploader.initialize(configBuilder.build());
    uploader.s3Client = mockS3Client;

    String expectedRemotePath = "path/prefix/test-topology/topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.getUrl(expectedBucket, expectedRemotePath)).thenReturn(new URL("http://url"));

    uploader.uploadPackage();

    verify(mockS3Client).putObject(Mockito.eq("bucket"),
        Mockito.eq(expectedRemotePath), Mockito.any(File.class));
  }

}
