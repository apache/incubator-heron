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

package org.apache.heron.spi.uploader;

import java.net.URI;

import org.apache.heron.spi.common.Config;

/**
 * Uploads topology package to a shared location. This location must be
 * accessible by runtime environment of topology. The uploader will upload
 * <p>
 * - topology jar,
 * - topology jar dependencies,
 * - topology definition, and
 * - heron core packages and libraries, if required
 * <p>
 * Uploader outputs another context containing the necessary information that
 * will be used by next stages of topology submission.
 * <p>
 * Implementation of IUploader is required to have a no argument constructor
 * that will be called to create an instance of IUploader.
 */
public interface IUploader extends AutoCloseable {
  /**
   * Initialize the uploader with the incoming context.
   * @param config The config object.
   */
  void initialize(Config config);

  /**
   * UploadPackage will upload the topology package to the given location.
   *
   * @return destination URI of where the topology package has
   * been uploaded if successful, or {@code null} if failed.
   */
  URI uploadPackage() throws UploaderException;

  /**
   * If subsequent stages fail, undo will be called to free resources used by
   * uploading package. Ideally, this should try to remove the uploaded package.
   * @return True if successful.
   */
  boolean undo();

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the uploader
   * <p>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();
}
