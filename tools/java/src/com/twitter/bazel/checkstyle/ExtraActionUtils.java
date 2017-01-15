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
package com.twitter.bazel.checkstyle;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.devtools.build.lib.actions.extra.ExtraActionInfo;
import com.google.devtools.build.lib.actions.extra.ExtraActionsBase;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistry;

/**
 * Utility methods for checkstyles
 */
public final class ExtraActionUtils {

  private ExtraActionUtils() {
  }

  public static ExtraActionInfo getExtraActionInfo(String extraActionFile) {
    ExtensionRegistry registry = ExtensionRegistry.newInstance();
    ExtraActionsBase.registerAllExtensions(registry);

    try (InputStream stream = Files.newInputStream(Paths.get(extraActionFile))) {
      CodedInputStream coded = CodedInputStream.newInstance(stream);
      return ExtraActionInfo.parseFrom(coded, registry);
    } catch (IOException e) {
      throw new RuntimeException("ERROR: failed to deserialize extra action file "
        + extraActionFile + ": " + e.getMessage(), e);
    }
  }
}
