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

package org.apache.heron.uploader.gcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;

public class GcsController {
  private static final String MIME_TYPE = "application/x-gzip";
  private static final String DEFAULT_PREDEFINED_ACL = "publicRead";

  private final Storage storage;
  private final String bucket;

  GcsController(Storage storage, String bucket) {
    this.storage = storage;
    this.bucket = bucket;
  }

  StorageObject createStorageObject(String storageObjectName, File file) throws IOException {
    final InputStreamContent content =
        new InputStreamContent(MIME_TYPE, new FileInputStream(file));
    final Storage.Objects.Insert insertStorageObject =
        storage.objects()
            .insert(bucket, null, content)
            .setName(storageObjectName)
            .setPredefinedAcl(DEFAULT_PREDEFINED_ACL);

    // The media uploader gzips content by default, and alters the Content-Encoding accordingly.
    // GCS dutifully stores content as-uploaded. This line disables the media uploader behavior,
    // so the service stores exactly what is in the InputStream, without transformation.
    insertStorageObject.getMediaHttpUploader().setDisableGZipContent(true);
    return insertStorageObject.execute();
  }

  StorageObject getStorageObject(String storageObjectName) {
    try {
      return storage.objects().get(bucket, storageObjectName).execute();
    } catch (IOException e) {
      // ignored
    }
    return null;
  }

  void copyStorageObject(String sourceStorageObjectName, String destinationStorageObjectName,
                  StorageObject storageObject) throws IOException {
    storage.objects()
        .copy(bucket, sourceStorageObjectName, bucket, destinationStorageObjectName, storageObject)
        .execute();
  }

  void deleteStorageObject(String storageObjectName) throws IOException {
    storage.objects().delete(bucket, storageObjectName).execute();
  }

  static GcsController create(Storage storage, String bucket) {
    return new GcsController(storage, bucket);
  }
}
