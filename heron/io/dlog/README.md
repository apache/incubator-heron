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
java -jar ./bazel-genfiles/heron/io/dlog/src/java/dlog-util.jar distributedlog://127.0.0.1/path/to/stream /path/to/file
```

4. Download a dlog stream as a file
```
java -jar ./bazel-genfiles/heron/io/dlog/src/java/dlog-util.jar distributedlog://127.0.0.1/path/to/stream /path/to/file
```
