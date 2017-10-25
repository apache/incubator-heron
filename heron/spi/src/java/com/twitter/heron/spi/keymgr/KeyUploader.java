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

package com.twitter.heron.spi.keymgr;

import java.nio.file.Path;

import com.twitter.heron.spi.common.Config;

/**
 * The KeyUploader interface. <p>
 * Implementations of this interface upload the keys/certificate/truststore wherever
 * they want to store securely.
 */
public interface KeyUploader extends AutoCloseable {
  /**
   * Initialize the KeyUploader with the static config
   *
   * @param config An unmodifiableMap containing basic configuration
   */
  void init(Config config);

  /**
   * download the certificate to file pointed by destinationFile
   *
   * @param certificateFile The certificate file that needs to be uploaded
   * @param keysFile The keys file that needs to be updated
   * @param trustStoreFile The trustStore file that needs to be updated
   */
  void upload(Path certificateFile, Path keysFile, Path trustStoreFile);

  /**
   * Any cleanups should be done here
   */
  void close();
}
