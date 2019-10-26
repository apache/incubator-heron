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

package org.apache.heron.uploader.http;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.uploader.IUploader;
import org.apache.heron.spi.uploader.UploaderException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * Provides a basic uploader class for uploading topology packages via Http.
 */
public class HttpUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(HttpUploader.class.getName());

  private static final String FILE = "file";
  private Config config;
  private String topologyPackageLocation;
  private HttpPost post;

  @Override
  @SuppressWarnings("HiddenField")
  public void initialize(Config config) {
    String uploaderUri = HttpUploaderContext.getHeronUploaderHttpUri(config);
    Preconditions.checkArgument(StringUtils.isNotBlank(uploaderUri),
        HttpUploaderContext.HERON_UPLOADER_HTTP_URI + " property must be set");

    String topologyPackageFile = Context.topologyPackageFile(config);
    Preconditions.checkArgument(StringUtils.isNotBlank(topologyPackageFile),
        Key.TOPOLOGY_PACKAGE_FILE + " property must be set");

    this.config = config;
    this.topologyPackageLocation = topologyPackageFile;
  }

  @Override
  public URI uploadPackage() throws UploaderException {
    try (CloseableHttpClient httpclient = HttpClients.custom().build()) {
      return uploadPackageAndGetURI(httpclient);
    } catch (IOException | URISyntaxException e) {
      String msg = "Error uploading package to location: " + this.topologyPackageLocation;
      LOG.log(Level.SEVERE, msg, e);
      throw new UploaderException(msg, e);
    }
  }

  private URI uploadPackageAndGetURI(final CloseableHttpClient httpclient)
      throws IOException, URISyntaxException {
    File file = new File(this.topologyPackageLocation);
    String uploaderUri = HttpUploaderContext.getHeronUploaderHttpUri(this.config);
    post = new HttpPost(uploaderUri);
    FileBody fileBody = new FileBody(file, ContentType.DEFAULT_BINARY);
    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
    builder.addPart(FILE, fileBody);
    HttpEntity entity = builder.build();
    post.setEntity(entity);
    HttpResponse response = execute(httpclient);
    String responseString = EntityUtils.toString(response.getEntity(),
        StandardCharsets.UTF_8.name());
    LOG.fine("Topology package download URI: " + responseString);

    return new URI(responseString);
  }

  protected HttpResponse execute(final HttpClient client) throws IOException {
    return client.execute(post);
  }

  protected HttpPost getPost() {
    return post;
  }

  @Override
  public boolean undo() {
    return false;
  }

  @Override
  public void close() {

  }
}
