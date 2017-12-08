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

public class HttpUploader implements IUploader{
  private static final Logger LOG = Logger.getLogger(HttpUploader.class.getName());

  private Config config;
  private String topologyPackageLocation;

  static {
    java.util.logging.Logger.getLogger("org.apache.http.wire")
        .setLevel(java.util.logging.Level.FINEST);
    java.util.logging.Logger.getLogger("org.apache.http.headers")
        .setLevel(java.util.logging.Level.FINEST);
    System.setProperty("org.apache.commons.logging.Log",
        "org.apache.commons.logging.impl.SimpleLog");
    System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
    System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire", "ERROR");
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "ERROR");
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.headers", "ERROR");
  }

  @Override
  public void initialize(Config config) {
    this.config = config;
    this.topologyPackageLocation = Context.topologyPackageFile(config);
  }

  @Override
  public URI uploadPackage() throws UploaderException {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    URI uri = null;
    try {
      HttpClient client = HttpClients.custom().build();

      File file = new File(this.topologyPackageLocation);
      String uploadeUri = HttpUploaderContext.getHeronUploaderHttpUri(this.config);
      HttpPost post = new HttpPost(uploadeUri);
      FileBody fileBody = new FileBody(file, ContentType.DEFAULT_BINARY);
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
      builder.addPart("file", fileBody);
      HttpEntity entity = builder.build();
      post.setEntity(entity);
      HttpResponse response = client.execute(post);
      String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");
      LOG.fine("Topology package download URI: " + responseString);
      uri = new URI(responseString);

    } catch (IOException | URISyntaxException e) {
      String msg = "Error uploading package " + this.topologyPackageLocation;
      LOG.log(Level.SEVERE, msg, e);
      throw new RuntimeException(msg, e);
    } finally {
      try {
        httpclient.close();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Error closing http client", e);
      }
    }
    return uri;
  }

  @Override
  public boolean undo() {
    return false;
  }

  @Override
  public void close() {

  }
}
