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
 * The KeyDownloader interface. <p>
 * Implementations of this interface download the keys/certificate/truststore to paths
 * passed in the various download interfaces.
 */
public interface KeyDownloader extends AutoCloseable {
  /**
   * Initialize the KeyDownloader
   *
   * @param config An unmodifiableMap containing basic configuration
   */
  void init(Config config);

  /**
   * download the certificate/keys/trustStore to file pointed
   *
   * @param certificateFile The file to download the certificate to
   * @param keysFile The file to download the keys to
   * @param trustStoreFile The file to download the trust Store to
   */
  void download(Path certificateFile, Path keysFile, Path trustStoreFile);

  /**
   * Any cleanups should be done here
   */
  void close();
}
