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

package com.twitter.heron.common.basics;

/***
 * This enum defines commands topology package type
 */
public enum PackageType {
  PEX,
  JAR,
  TAR;

  private static boolean isOriginalPackageJar(String packageFilename) {
    return packageFilename.endsWith(".jar");
  }

  private static boolean isOriginalPackagePex(String packageFilename) {
    return packageFilename.endsWith(".pex");
  }

  private static boolean isOriginalPackageTar(String packageFilename) {
    return packageFilename.endsWith(".tar");
  }

  public static PackageType getPackageType(String topologyBinaryFile) {
    String basename = FileUtils.getBaseName(topologyBinaryFile);
    if (isOriginalPackagePex(basename)) {
      return PackageType.PEX;
    } else if (isOriginalPackageJar(basename)) {
      return PackageType.JAR;
    } else if (isOriginalPackageTar(basename)) {
      return PackageType.TAR;
    } else {
      throw new RuntimeException(String.format("Unknown package type of file: %s",
          topologyBinaryFile));
    }
  }

}
