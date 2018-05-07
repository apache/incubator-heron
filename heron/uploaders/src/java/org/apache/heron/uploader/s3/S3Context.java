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

import com.amazonaws.regions.Regions;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;

public class S3Context extends Context {
  public static final String HERON_UPLOADER_S3_AWS_PROFILE = "heron.uploader.s3.aws_profile";
  public static final String HERON_UPLOADER_S3_ACCESS_KEY = "heron.uploader.s3.access_key";
  public static final String HERON_UPLOADER_S3_SECRET_KEY = "heron.uploader.s3.secret_key";
  public static final String HERON_UPLOADER_S3_BUCKET = "heron.uploader.s3.bucket";
  public static final String HERON_UPLOADER_S3_PATH_PREFIX = "heron.uploader.s3.path_prefix";
  public static final String HERON_UPLOADER_S3_PROXY_URI = "heron.uploader.s3.proxy_uri";
  public static final String HERON_UPLOADER_S3_URI = "heron.uploader.s3.uri";
  public static final String HERON_UPLOADER_S3_REGION = "heron.uploader.s3.region";



  public static String pathPrefix(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_PATH_PREFIX, "/");
  }

  public static String bucket(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_BUCKET);
  }

  public static String awsProfile(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_AWS_PROFILE);
  }

  public static String accessKey(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_ACCESS_KEY);
  }

  public static String secretKey(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_SECRET_KEY);
  }

  public static String region(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_REGION, Regions.DEFAULT_REGION.getName());
  }

  public static String uri(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_URI);
  }

  public static String proxyUri(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_PROXY_URI);
  }

}

