//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.uploader.gcs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.api.services.storage.model.StorageObject;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;

import static org.junit.Assert.assertEquals;

public class GcsUploaderTests {

  private final String topologyName = "test-topology";

  private final String topologyPackageName = "topology.tar.gz";

  private final String bucket = "topologies-bucket";

  private final String topologyObjectName =
      String.format("%s/%s", topologyName, topologyPackageName);

  private final String previousTopologyObjectName =
      String.format("%s/previous-%s", topologyName, topologyPackageName);

  private GcsUploader uploader;

  private GcsController mockGcsController = Mockito.mock(GcsController.class);

  @Before
  public void before() throws Exception {
    uploader = createUploaderWithMockController();
  }

  @Test
  public void uploadTopology() throws IOException, URISyntaxException {
    Mockito.when(mockGcsController
        .createObject(Mockito.matches(topologyObjectName), Mockito.any(File.class)))
        .thenReturn(createObject(topologyObjectName));

    uploader.initialize(createDefaultBuilder().build());

    String expectedUri =
        String.format("https://storage.googleapis.com/%s/%s/%s",
            bucket, topologyName, topologyPackageName);
    assertEquals(new URI(expectedUri), uploader.uploadPackage());
  }

  @Test
  public void verifyObjectBackedUpIfExists() throws IOException {
    // return an object to simulate that the topology has been uploaded before
    final StorageObject currentObject = createObject(topologyObjectName);
    Mockito.when(mockGcsController
        .getObject(Mockito.matches(topologyObjectName)))
        .thenReturn(currentObject);

    // return an object when we try to create one
    Mockito.when(mockGcsController
        .createObject(Mockito.matches(topologyObjectName), Mockito.any(File.class)))
        .thenReturn(createObject(topologyObjectName));

    uploader.initialize(createDefaultBuilder().build());

    uploader.uploadPackage();

    // verify that we copied the old topology before uploading the new one
    Mockito.verify(mockGcsController)
        .copyObject(topologyObjectName, previousTopologyObjectName, currentObject);
  }

  @Test
  public void restorePreviousVersionOnUndo() throws IOException {
    final StorageObject previousObject = createObject(previousTopologyObjectName);
    Mockito.when(mockGcsController
        .getObject(Mockito.matches(previousTopologyObjectName)))
        .thenReturn(previousObject);

    uploader.initialize(createDefaultBuilder().build());

    uploader.undo();

    // verify that we restored the previous topology
    Mockito.verify(mockGcsController)
        .copyObject(previousTopologyObjectName, topologyObjectName, previousObject);
  }

  @Test
  public void doNotRestorePreviousVersionIfItDoesNotExist() throws IOException {
    Mockito.when(mockGcsController
        .getObject(Mockito.matches(previousTopologyObjectName)))
        .thenReturn(null);

    uploader.initialize(createDefaultBuilder().build());

    uploader.undo();

    Mockito.verify(mockGcsController, Mockito.never())
        .copyObject(Mockito.anyString(), Mockito.anyString(), Mockito.any(StorageObject.class));
  }

  private StorageObject createObject(String name) {
    final StorageObject storageObject = new StorageObject();
    storageObject.setName(name);
    return storageObject;
  }

  private Config.Builder createDefaultBuilder() {
    return Config.newBuilder()
        .put(GcsContext.HERON_UPLOADER_GCS_BUCKET, bucket)
        .put(Key.TOPOLOGY_NAME, topologyName)
        .put(Key.TOPOLOGY_PACKAGE_FILE, topologyPackageName);
  }

  private GcsUploader createUploaderWithMockController() {
    return new GcsUploader() {
      @Override
      GcsController createGcsController(Config configuration, String storageBucket) {
        return mockGcsController;
      }
    };
  }
}
