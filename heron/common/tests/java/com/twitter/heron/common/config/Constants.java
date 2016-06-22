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

package com.twitter.heron.common.config;

public final class Constants {
  public static final String ROLE_KEY = "role";
  public static final String ENVIRON_KEY = "environ";
  public static final String USER_KEY = "user";
  public static final String GROUP_KEY = "group";
  public static final String VERSION_KEY = "version";

  public static final String LAUNCHER_CLASS_KEY = "heron.launcher.class";

  public static final String TEST_DATA_PATH =
      "/__main__/heron/common/tests/java/com/twitter/heron/common/config/testdata";

  private Constants() {
  }
}
