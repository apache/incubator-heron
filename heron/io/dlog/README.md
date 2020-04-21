<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
Apache BookKeeper/DistributedLog
================================

This directory provides the common utils for accessing distributedlog streams.

### Util

Utils is a tool for copying data from local filesytem to dlog or copy dlog stream to local filesytem. This tool is useful for verifying dlog based downloader and uploader.

1. Build the tool.
```
bazel build heron/io/dlog/src/java:dlog-util
```

2. You can run a bookkeeper sandbox with 3 bookies locally following the instructions [here](http://bookkeeper.apache.org/docs/latest/getting-started/run-locally/).

3. Upload a file to dlog.
```
java -jar ./bazel-bin/heron/io/dlog/src/java/dlog-util.jar distributedlog://127.0.0.1/path/to/stream /path/to/file
```

4. Download a dlog stream as a file
```
java -jar ./bazel-bin/heron/io/dlog/src/java/dlog-util.jar distributedlog://127.0.0.1/path/to/stream /path/to/file
```
