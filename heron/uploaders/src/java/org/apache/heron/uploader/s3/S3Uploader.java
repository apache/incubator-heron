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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.base.Strings;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.uploader.IUploader;
import org.apache.heron.spi.uploader.UploaderException;

/**
 * Provides a basic uploader class for uploading topology packages to S3.
 * <p>
 * By default this uploader will write topology packages to s3://&lt;bucket&gt;/&lt;topologyName&gt;/topology.tar.gz
 * trying to obtain credentials using the default credential provider chain. The package destination serves as known
 * location which can be used to download the topology package in order to run the topology.
 * <p>
 * This class also handles the undo action by copying any existing topology.tar.gz package found in the folder to
 * previous_topology.tar.gz. In the event that the deploy fails and the undo action is triggered the previous_topology.tar.gz
 * file will be renamed to topology.tar.gz effectively rolling back the live code. In the event that the deploy is successful
 * the previous_topology.tar.gz package will be deleted as it is no longer needed.
 * <p>
 * The config values for this uploader are:
 * heron.class.uploader (required) org.apache.heron.uploader.s3.S3Uploader
 * heron.uploader.s3.bucket (required) The bucket that you have write access to where you want the topology packages to be stored
 * heron.uploader.s3.path_prefix (optional) Optional prefix for the path to the topology packages
 * heron.uploader.s3.access_key (optional) S3 access key that can be used to write to the bucket provided
 * heron.uploader.s3.secret_key (optional) S3 access secret that can be used to write to the bucket provided
 * heron.uploader.s3.aws_profile (optional) AWS profile to use
 */
public class S3Uploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(S3Uploader.class.getName());

  private String bucket;
  protected AmazonS3 s3Client;
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
    String awsProfile = S3Context.awsProfile(config);
    String proxy = S3Context.proxyUri(config);
    String endpoint = S3Context.uri(config);
    String customRegion = S3Context.region(config);
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

    if (Strings.isNullOrEmpty(bucket)) {
      throw new RuntimeException("Missing heron.uploader.s3.bucket config value");
    }

    // If an accessKey is specified, use it. Otherwise check if an aws profile
    // is specified. If neither was set just use the DefaultAWSCredentialsProviderChain
    // by not specifying a CredentialsProvider.
    if (!Strings.isNullOrEmpty(accessKey) || !Strings.isNullOrEmpty(accessSecret)) {

      if (!Strings.isNullOrEmpty(awsProfile)) {
        throw new RuntimeException("Please provide access_key/secret_key "
            + "or aws_profile, not both.");
      }

      if (Strings.isNullOrEmpty(accessKey)) {
        throw new RuntimeException("Missing heron.uploader.s3.access_key config value");
      }

      if (Strings.isNullOrEmpty(accessSecret)) {
        throw new RuntimeException("Missing heron.uploader.s3.secret_key config value");
      }
      builder.setCredentials(
              new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, accessSecret))
      );
    } else if (!Strings.isNullOrEmpty(awsProfile)) {
      builder.setCredentials(new ProfileCredentialsProvider(awsProfile));
    }

    if (!Strings.isNullOrEmpty(proxy)) {
      URI proxyUri;

      try  {
        proxyUri = new URI(proxy);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Invalid heron.uploader.s3.proxy_uri config value: " + proxy, e);
      }

      ClientConfiguration clientCfg = new ClientConfiguration();
      clientCfg.withProtocol(Protocol.HTTPS)
              .withProxyHost(proxyUri.getHost())
              .withProxyPort(proxyUri.getPort());

      if (!Strings.isNullOrEmpty(proxyUri.getUserInfo())) {
        String[] info = proxyUri.getUserInfo().split(":", 2);
        clientCfg.setProxyUsername(info[0]);
        if (info.length > 1) {
          clientCfg.setProxyPassword(info[1]);
        }
      }

      builder.setClientConfiguration(clientCfg);
    }

    s3Client = builder.withRegion(customRegion)
            .withPathStyleAccessEnabled(true)
            .withChunkedEncodingDisabled(true)
            .withPayloadSigningEnabled(true)
            .build();

    if (!Strings.isNullOrEmpty(endpoint)) {
      s3Client.setEndpoint(endpoint);
    }

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
  public URI uploadPackage() throws UploaderException {
    // Backup any existing files incase we need to undo this action
    if (s3Client.doesObjectExist(bucket, remoteFilePath)) {
      s3Client.copyObject(bucket, remoteFilePath, bucket, previousVersionFilePath);
    }

    // Attempt to write the topology package to s3
    try {
      s3Client.putObject(bucket, remoteFilePath, packageFileHandler);
    } catch (SdkClientException e) {
      throw new UploaderException(
          String.format("Error writing topology package to %s %s", bucket, remoteFilePath), e);
    }

    // Ask s3 for the url to the topology package we just uploaded
    final URL resourceUrl = s3Client.getUrl(bucket, remoteFilePath);
    LOG.log(Level.INFO, "Package URL: {0}", resourceUrl);

    // This will happen if the package does not actually exist in the place where we uploaded it to.
    if (resourceUrl == null) {
      throw new UploaderException(
          String.format("Resource not found for bucket %s and path %s", bucket, remoteFilePath));
    }

    try {
      return resourceUrl.toURI();
    } catch (URISyntaxException e) {
      throw new UploaderException(
          String.format("Could not convert URL %s to URI", resourceUrl), e);
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
      } catch (SdkClientException e) {
        LOG.log(Level.SEVERE, "Reverting to previous topology version failed", e);
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() {
    // Cleanup the backup file if it exists as its not needed anymore.
    // This will succeed whether the file exists or not.
    if (!Strings.isNullOrEmpty(previousVersionFilePath)) {
      s3Client.deleteObject(bucket, previousVersionFilePath);
    }
  }
}
