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

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;

public class GcsContext extends Context {
  static final String HERON_UPLOADER_GCS_CREDENTIALS_PATH =
      "heron.uploader.gcs.credentials_path";
  static final String HERON_UPLOADER_GCS_BUCKET = "heron.uploader.gcs.bucket";

  static String getBucket(Config configuration) {
    return configuration.getStringValue(HERON_UPLOADER_GCS_BUCKET);
  }

  static String getCredentialsPath(Config configuration) {
    return configuration.getStringValue(HERON_UPLOADER_GCS_CREDENTIALS_PATH);
  }
}
