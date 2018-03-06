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
package com.twitter.heron.uploader.http;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

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

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.uploader.UploaderException;

/**
 * Provides a basic uploader class for uploading topology packages via Http.
 */
public class HttpUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(HttpUploader.class.getName());

  private Config config;
  private String topologyPackageLocation;
  private HttpPost post;

  @Override
  @SuppressWarnings("HiddenField")
  public void initialize(Config config) {
    this.config = config;
    this.topologyPackageLocation = Context.topologyPackageFile(config);
  }

  @Override
  public URI uploadPackage() throws UploaderException {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    URI uri;

    try {
      HttpClient client = HttpClients.custom().build();

      File file = new File(this.topologyPackageLocation);
      String uploaderUri = HttpUploaderContext.getHeronUploaderHttpUri(this.config);
      post = new HttpPost(uploaderUri);
      FileBody fileBody = new FileBody(file, ContentType.DEFAULT_BINARY);
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
      builder.addPart("file", fileBody);
      HttpEntity entity = builder.build();
      post.setEntity(entity);
      HttpResponse response = execute(client);
      String responseString = EntityUtils.toString(response.getEntity(),
          StandardCharsets.UTF_8.name());
      LOG.fine("Topology package download URI: " + responseString);
      uri = new URI(responseString);
    } catch (IOException | URISyntaxException e) {
      String msg = "Error uploading package " + this.topologyPackageLocation;
      LOG.log(Level.SEVERE, msg, e);
      throw new UploaderException(msg, e);
    } finally {
      try {
        httpclient.close();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Error closing http client", e);
      }
    }

    return uri;
  }

  protected HttpResponse execute(HttpClient client) throws IOException {
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
