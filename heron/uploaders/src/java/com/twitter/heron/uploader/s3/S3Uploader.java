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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.uploader.IUploader;

/**
 * Provides a basic uploader class for uploading topology packages to s3.
 * <p>
 * By default this uploader will write topology packages to s3://&lt;bucket&gt;/&lt;topologyName&gt;/topology.tar.gz. This is a known
 * location where you can then download the topology package to where it needs to go in order to run the topology.
 * <p>
 * This class also handles the undo action by copying any existing topology.tar.gz package found in the folder to
 * previous_topology.tar.gz. In the event that the deploy fails and the undo action is triggered the previous_topology.tar.gz
 * file will be renamed to topology.tar.gz effectively rolling back the live code. In the event that the deploy is successful
 * the previous_topology.tar.gz package will be deleted as it is no longer needed.
 * <p>
 * The config values for this uploader are:
 * heron.class.uploader (required) com.twitter.heron.uploader.s3.S3Uploader
 * heron.uploader.s3.bucket (required) The bucket that you have write access to where you want the topology packages to be stored
 * heron.uploader.s3.path_prefix (optional) Optional prefix for the path to the topology packages
 * heron.uploader.s3.access_key (required) S3 access key that can be used to write to the bucket provided
 * heron.uploader.s3.secret_key (required) S3 access secret that can be used to write to the bucket provided
 */
public class S3Uploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(S3Uploader.class.getName());

  private String bucket;
  protected AmazonS3Client s3Client;
  private String remoteFilePath;

  // The path prefix will be prepended to the path inside the provided bucket.
  // For example if you set bucket=foo and path_prefix=some/sub/folder then you
  // can expect the final path in s3 to look like:
  // s3://foo/some/sub/folder/<topologyName>/topology.tar.gz
  private String pathPrefix;

  // Stores the path to the backup version of the topology in case the deploy fails and
  // it needs to be undone. This is the same as the existing path but prepended with `previous_`.
  // This serves as a simple backup incase we need to revert.
  private String previousVersionFilePath;

  private File packageFileHandler;

  @Override
  public void initialize(Config config) {
    bucket = S3Context.bucket(config);
    String accessKey = S3Context.accessKey(config);
    String accessSecret = S3Context.secretKey(config);

    if (bucket == null || bucket.isEmpty()) {
      throw new RuntimeException("Missing heron.uploader.s3.bucket config value");
    }

    if (accessKey == null || accessKey.isEmpty()) {
      throw new RuntimeException("Missing heron.uploader.s3.access_key config value");
    }

    if (accessSecret == null || accessSecret.isEmpty()) {
      throw new RuntimeException("Missing heron.uploader.s3.secret_key config value");
    }

    AWSCredentials credentials = new BasicAWSCredentials(accessKey, accessSecret);
    s3Client = new AmazonS3Client(credentials);

    final String topologyName = Context.topologyName(config);
    final String topologyPackageLocation = Context.topologyPackageFile(config);

    pathPrefix = S3Context.pathPrefix(config);
    packageFileHandler = new File(topologyPackageLocation);

    // The path the packaged topology will be uploaded to
    remoteFilePath = generateS3Path(pathPrefix, topologyName, packageFileHandler.getName());

    // Generate the location of the backup file incase we need to revert the deploy
    previousVersionFilePath = generateS3Path(pathPrefix, topologyName,
        "previous_" + packageFileHandler.getName());
  }

  @Override
  public URI uploadPackage() {
    // Backup any existing files incase we need to undo this action
    if (s3Client.doesObjectExist(bucket, remoteFilePath)) {
      s3Client.copyObject(bucket, remoteFilePath, bucket, previousVersionFilePath);
    }

    // Attempt to write the topology package to s3
    try {
      s3Client.putObject(bucket, remoteFilePath, packageFileHandler);
    } catch (AmazonClientException e) {
      LOG.log(Level.SEVERE, "Error writing topology package to " + bucket + " "
          + remoteFilePath, e);
      return null;
    }

    // Ask s3 for the url to the topology package we just uploaded
    final String resourceUrl = s3Client.getResourceUrl(bucket, remoteFilePath);
    LOG.log(Level.INFO, "Package URL: {0}", resourceUrl);

    // This will happen if the package does not actually exist in the place where we uploaded it to.
    if (resourceUrl == null) {
      LOG.log(Level.SEVERE, "Resource not found for bucket " + bucket + " and path "
          + remoteFilePath);
      return null;
    }

    try {
      return new URI(resourceUrl);
    } catch (URISyntaxException e) {
      LOG.log(Level.SEVERE, e.getMessage());
      return null;
    }
  }

  /**
   * Generate the path to a file in s3 given a prefix, topologyName, and filename
   *
   * @param pathPrefixParent designates any parent folders that should be prefixed to the resulting path
   * @param topologyName the name of the topology that we are uploaded
   * @param filename the name of the resulting file that is going to be uploaded
   * @return the full path of the package under the bucket. The bucket is not included in this path as it is a separate
   * argument that is passed to the putObject call in the s3 sdk.
   */
  private String generateS3Path(String pathPrefixParent, String topologyName, String filename) {
    List<String> pathParts = new ArrayList<>(Arrays.asList(pathPrefixParent.split("/")));
    pathParts.add(topologyName);
    pathParts.add(filename);

    return String.join("/", pathParts);
  }

  @Override
  public boolean undo() {
    // Check if there is a previous version. This will not be true on the first deploy.
    if (s3Client.doesObjectExist(bucket, previousVersionFilePath)) {
      try {
        // Restore the previous version of the topology
        s3Client.copyObject(bucket, previousVersionFilePath, bucket, remoteFilePath);
      } catch (AmazonClientException e) {
        LOG.log(Level.SEVERE, "Error undoing deploying", e);
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() {
    // Cleanup the backup file if it exists as its not needed anymore.
    // This will succeed whether the file exists or not.
    s3Client.deleteObject(bucket, previousVersionFilePath);
  }
}
