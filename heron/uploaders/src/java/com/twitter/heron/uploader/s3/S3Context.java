package com.twitter.heron.uploader.s3;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class S3Context extends Context {
  public static final String HERON_UPLOADER_S3_BUCKET = "heron.uploader.s3.bucket";
  public static final String HERON_UPLOADER_S3_ACCESS_KEY = "heron.uploader.s3.access_key";
  public static final String HERON_UPLOADER_S3_SECRET_KEY = "heron.uploader.s3.secret_key";
  public static final String HERON_UPLOADER_S3_PATH_PREFIX = "heron.uploader.s3.path_prefix";

  public static String pathPrefix(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_PATH_PREFIX, "/");
  }

  public static String bucket(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_BUCKET);
  }

  public static String accessKey(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_ACCESS_KEY);
  }

  public static String secretKey(Config config) {
    return config.getStringValue(HERON_UPLOADER_S3_SECRET_KEY);
  }
}
