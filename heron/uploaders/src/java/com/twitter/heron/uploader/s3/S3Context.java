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

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.SpiCommonConfig;

public class S3Context extends Context {
  public static final String HERON_UPLOADER_S3_BUCKET = "heron.uploader.s3.bucket";
  public static final String HERON_UPLOADER_S3_ACCESS_KEY = "heron.uploader.s3.access_key";
  public static final String HERON_UPLOADER_S3_SECRET_KEY = "heron.uploader.s3.secret_key";
  public static final String HERON_UPLOADER_S3_PATH_PREFIX = "heron.uploader.s3.path_prefix";

  public static String pathPrefix(SpiCommonConfig config) {
    return config.getStringValue(HERON_UPLOADER_S3_PATH_PREFIX, "/");
  }

  public static String bucket(SpiCommonConfig config) {
    return config.getStringValue(HERON_UPLOADER_S3_BUCKET);
  }

  public static String accessKey(SpiCommonConfig config) {
    return config.getStringValue(HERON_UPLOADER_S3_ACCESS_KEY);
  }

  public static String secretKey(SpiCommonConfig config) {
    return config.getStringValue(HERON_UPLOADER_S3_SECRET_KEY);
  }
}
