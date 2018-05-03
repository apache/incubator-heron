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
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Strings;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.uploader.IUploader;
import org.apache.heron.spi.uploader.UploaderException;

/**
 * Provides a basic uploader class for uploading topology packages to Google Cloud Storage (gcs).
 * <p>
 * This uploader will write topology packages to
 * https://storage.googleapis.com/bucket-name/topology-name/topology.tar.gz
 * This provides a known package destination location which can be used to download
 * the topology package in order to run the topology.
 *
 * Clients must have write access to the bucket in order to upload topologies. Authentication
 * can be provided in two ways:
 * 1 - A file path to the service account credentials file (https://cloud.google.com/storage/docs/authentication#service_accounts)
 * example: heron.uploader.gcs.credentials_path: /Users/username/my-service-account-credentials.json
 * 2 - through the gcloud client (https://cloud.google.com/storage/docs/authentication#libauth)
 * example: gcloud auth application-default login
 * Option 2 is used when a heron.uploader.gcs.credentials_path: is not provided.
 *
 * <p>
 * This class also handles the undo action by copying any existing topology.tar.gz package found
 * in the folder to previous-topology.tar.gz. In the event that the deploy fails and
 * the undo action is triggered the previous-topology.tar.gz file will be renamed
 * to topology.tar.gz effectively rolling back the live code. In the event that the deploy is
 * successful the previous-topology.tar.gz package will be deleted as it is no longer needed.
 * <p>
 * The config values for this uploader are:
 * heron.class.uploader (required) org.apache.heron.uploader.gcs.GcsUploader
 * heron.uploader.gcs.bucket (required) The bucket that you have write access to where you want the topology packages to be stored
 * heron.uploader.gcs.credentials_path (optional) Google Services Account Credentials to use
 */
public class GcsUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(GcsUploader.class.getName());

  private static final String BACKUP_PREFIX = "previous-";
  private static final String GCS_URL_FORMAT = "https://storage.googleapis.com/%s/%s";

  private GcsController gcsController;
  private String bucket;
  private File topologyPackageFile;
  private String topologyObjectName;

  // Stores the path to the backup version of the topology in case the deploy fails and
  // it needs to be undone. This is the same as the existing path but prepended with `previous-`.
  // This serves as a simple backup incase we need to revert.
  private String previousTopologyObjectName;

  @Override
  public void initialize(Config config) {
    bucket = GcsContext.getBucket(config);

    if (Strings.isNullOrEmpty(bucket)) {
      throw new RuntimeException("Missing heron.uploader.gcs.bucket config value");
    }

    // get the topology package location
    final String topologyPackageLocation = Context.topologyPackageFile(config);
    final String topologyName = Context.topologyName(config);

    topologyPackageFile = new File(topologyPackageLocation);
    final String topologyFilename = topologyPackageFile.getName();
    topologyObjectName = generateStorageObjectName(topologyName, topologyFilename);
    previousTopologyObjectName =
        generateStorageObjectName(topologyName, BACKUP_PREFIX + topologyFilename);

    try {
      gcsController = createGcsController(config, bucket);
    } catch (IOException | GeneralSecurityException ex) {
      throw new RuntimeException("Unable to create google storage client", ex);
    }
  }

  @Override
  public URI uploadPackage() throws UploaderException {
    // Backup any existing files incase we need to undo this action
    final StorageObject previousStorageObject =
        gcsController.getStorageObject(topologyObjectName);
    if (previousStorageObject != null) {
      try {
        gcsController.copyStorageObject(topologyObjectName, previousTopologyObjectName,
            previousStorageObject);
      } catch (IOException ioe) {
        throw new UploaderException("Failed to back up previous topology", ioe);
      }
    }

    final StorageObject storageObject;
    try {
      storageObject = gcsController.createStorageObject(topologyObjectName, topologyPackageFile);
    } catch (IOException ioe) {
      throw new UploaderException(
          String.format("Error writing topology package to %s %s",
              bucket, topologyObjectName), ioe);
    }

    final String downloadUrl = getDownloadUrl(bucket, storageObject.getName());
    LOG.info("Package URL: " + downloadUrl);
    try {
      return new URI(downloadUrl);
    } catch (URISyntaxException e) {
      throw new UploaderException(
          String.format("Could not convert URL %s to URI", downloadUrl), e);
    }
  }

  @Override
  public boolean undo() {
    // Try to get the previous version. This will be null on the first deploy.
    final StorageObject previousObject =
        gcsController.getStorageObject(previousTopologyObjectName);
    if (previousObject != null) {
      try {
        gcsController.copyStorageObject(previousTopologyObjectName, topologyObjectName,
            previousObject);
      } catch (IOException ioe) {
        LOG.log(Level.SEVERE, "Reverting to previous topology failed", ioe);
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() {
    if (!Strings.isNullOrEmpty(previousTopologyObjectName)) {
      try {
        gcsController.deleteStorageObject(previousTopologyObjectName);
      } catch (IOException ioe) {
        LOG.info("Failed to delete previous topology " + previousTopologyObjectName);
      }
    }
  }

  GcsController createGcsController(Config configuration, String storageBucket)
      throws IOException, GeneralSecurityException {
    final Credential credential = createCredentials(configuration);
    final Storage storage = createStorage(credential);

    return GcsController.create(storage, storageBucket);
  }

  private Credential createCredentials(Config configuration) throws IOException {
    final String credentialsPath = GcsContext.getCredentialsPath(configuration);
    if (!Strings.isNullOrEmpty(credentialsPath)) {
      LOG.info("Using credentials from file: " + credentialsPath);
      return GoogleCredential
          .fromStream(new FileInputStream(credentialsPath))
          .createScoped(StorageScopes.all());
    }

    // if a credentials path is not provided try using the application default one.
    LOG.info("Using default application credentials");
    return GoogleCredential.getApplicationDefault().createScoped(StorageScopes.all());
  }

  private Storage createStorage(Credential credential)
      throws GeneralSecurityException, IOException {
    final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    final JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    return new Storage.Builder(httpTransport, jsonFactory, credential).build();
  }

  /**
   * Generate the storage object name in gcs given the topologyName and filename.
   *
   * @param topologyName the name of the topology
   * @param filename the name of the file to upload to gcs
   * @return the name of the object.
   */
  private static String generateStorageObjectName(String topologyName, String filename) {
    return String.format("%s/%s", topologyName, filename);
  }

  /**
   * Returns a url to download an gcs object the given bucket and object name
   *
   * @param bucket the name of the bucket
   * @param objectName the name of the object
   * @return a url to download the object
   */
  private static String getDownloadUrl(String bucket, String objectName) {
    return String.format(GCS_URL_FORMAT, bucket, objectName);
  }
}
