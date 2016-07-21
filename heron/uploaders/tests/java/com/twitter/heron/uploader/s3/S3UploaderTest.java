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

package com.twitter.heron.uploader.s3;

import java.io.File;
import java.net.URI;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.common.ConfigKeys;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3UploaderTest {
  private S3Uploader uploader;
  private AmazonS3Client mockS3Client;
  private SpiCommonConfig.Builder configBuilder;

  @Before
  public void setUp() {
    mockS3Client = mock(AmazonS3Client.class);

    configBuilder = SpiCommonConfig.newBuilder()
        .put(S3Context.HERON_UPLOADER_S3_BUCKET, "bucket")
        .put(S3Context.HERON_UPLOADER_S3_ACCESS_KEY, "access_key")
        .put(S3Context.HERON_UPLOADER_S3_SECRET_KEY, "secret_key")
        .put(ConfigKeys.get("TOPOLOGY_NAME"), "test-topology")
        .put(ConfigKeys.get("TOPOLOGY_PACKAGE_FILE"), "topology.tar.gz");

    uploader = new S3Uploader();
    uploader.initialize(configBuilder.build());
    uploader.s3Client = mockS3Client;
  }

  @Test
  public void uploadTopologyToS3() throws Exception {
    String expectedRemotePath = "test-topology/topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.doesObjectExist(expectedBucket, expectedRemotePath)).thenReturn(false);
    when(mockS3Client.getResourceUrl(expectedBucket, expectedRemotePath)).thenReturn("http://url");

    URI uri = uploader.uploadPackage();

    verify(mockS3Client).putObject(Mockito.eq(expectedBucket),
        Mockito.eq(expectedRemotePath), Mockito.any(File.class));

    verify(mockS3Client).getResourceUrl(expectedBucket, expectedRemotePath);

    assertEquals(new URI("http://url"), uri);
  }

  @Test
  public void backupPreviousVersionOnDeployForRollback() throws Exception {
    String expectedRemotePath = "test-topology/topology.tar.gz";
    String expectedPreviousVersionPath = "test-topology/previous_topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.doesObjectExist(expectedBucket, expectedRemotePath)).thenReturn(true);
    when(mockS3Client.getResourceUrl(expectedBucket, expectedRemotePath)).thenReturn("http://url");

    URI uri = uploader.uploadPackage();

    verify(mockS3Client).copyObject(expectedBucket, expectedRemotePath, expectedBucket,
        expectedPreviousVersionPath);

    verify(mockS3Client).putObject(Mockito.eq(expectedBucket), Mockito.eq(expectedRemotePath),
        Mockito.any(File.class));

    verify(mockS3Client).getResourceUrl(expectedBucket, expectedRemotePath);

    assertEquals(new URI("http://url"), uri);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void handlePutObjectExceptionOnUpload() throws Exception {
    String expectedRemotePath = "test-topology/topology.tar.gz";
    String expectedBucket = "bucket";

    when(mockS3Client.doesObjectExist(expectedBucket, expectedRemotePath)).thenReturn(true);
    when(mockS3Client.putObject(Mockito.eq(expectedBucket), Mockito.eq(expectedRemotePath),
        Mockito.any(File.class))).thenThrow(AmazonClientException.class);

    URI uri = uploader.uploadPackage();
    assertEquals(null, uri);
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

    when(mockS3Client.getResourceUrl(expectedBucket, expectedRemotePath)).thenReturn("http://url");

    uploader.uploadPackage();

    verify(mockS3Client).putObject(Mockito.eq("bucket"),
        Mockito.eq(expectedRemotePath), Mockito.any(File.class));
  }

}
