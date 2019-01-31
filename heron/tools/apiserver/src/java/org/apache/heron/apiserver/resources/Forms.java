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

package org.apache.heron.apiserver.resources;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.ws.rs.core.MultivaluedMap;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import org.apache.heron.apiserver.utils.FileHelper;

final class Forms {

  static String getString(FormDataMultiPart form, String key) {
    return getValueAs(form, key, String.class);
  }

  static String getString(FormDataMultiPart form, String key, String defaultValue) {
    return form.getFields().containsKey(key) ? getValueAs(form, key, String.class) : defaultValue;
  }

  static String getFirstOrDefault(MultivaluedMap<String, String> params, String key,
        String defaultValue) {
    return params.containsKey(key) ? params.getFirst(key) : defaultValue;
  }

  static File uploadFile(FormDataBodyPart part, String directory) throws IOException {
    try (InputStream in = part.getValueAs(InputStream.class)) {
      Path path = Paths.get(directory, part.getFormDataContentDisposition().getFileName());
      FileHelper.copy(in, path);
      return path.toFile();
    }
  }

  private static <T> T getValueAs(FormDataMultiPart form, String key, Class<T> type) {
    return form.getField(key).getValueAs(type);
  }

  private Forms() {
  }
}
