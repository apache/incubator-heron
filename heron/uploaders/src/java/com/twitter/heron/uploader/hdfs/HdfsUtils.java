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

package com.twitter.heron.uploader.hdfs;

import com.twitter.heron.spi.common.ShellUtils;

public class HdfsUtils {
    private HdfsUtils() {

    }

    public static boolean isFileExists(String configDir, String fileURI, boolean isVerbose) {
        String command = String.format("hadoop --config %s fs -test -e %s", configDir, fileURI);
        return (0 == ShellUtils.runProcess(isVerbose, command, null, null));
    }

    public static boolean createDir(String configDir, String dir, boolean isVerbose) {
        String command = String.format("hadoop --config %s fs -mkdir -p %s", configDir, dir);
        return (0 == ShellUtils.runProcess(isVerbose, command, null, null));
    }

    public static boolean copyFromLocal(
            String configDir, String source, String target, boolean isVerbose) {
        String command = String.format("hadoop --config %s fs -copyFromLocal -f %s %s",
                configDir, source, target);
        return (0 == ShellUtils.runProcess(isVerbose, command, null, null));
    }

    public static boolean remove(String configDir, String fileURI, boolean isVerbose) {
        String command = String.format("hadoop --config %s fs -rm %s", configDir, fileURI);
        return (0 == ShellUtils.runProcess(isVerbose, command, null, null));
    }
}
