licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "libhdfs3"
package_version = "unversioned"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

include_files = [
    "include/hdfs/BlockLocation.h",
    "include/hdfs/DirectoryIterator.h",
    "include/hdfs/Exception.h",
    "include/hdfs/FileStatus.h",
    "include/hdfs/FileSystem.h",
    "include/hdfs/FileSystemStats.h",
    "include/hdfs/InputStream.h",
    "include/hdfs/OutputStream.h",
    "include/hdfs/Permission.h",
    "include/hdfs/XmlConfig.h",
    "include/hdfs/hdfs.h",
]

lib_files = [
    "lib/libhdfs3.a",
]

genrule(
    name = "libhdfs3-srcs",
    srcs = [
        package_file,
        "//third_party/gtest:gtest-files",
        "//third_party/gmock:gmock-files",
        "//third_party/protobuf:protobuf-files",
        "//third_party/gsasl:gsasl-files",
    ],
    outs = include_files + lib_files,
    cmd = "\n".join([
        'export GTEST_DIR=$$(pwd)/$(GENDIR)/third_party/gtest',
        'export GMOCK_DIR=$$(pwd)/$(GENDIR)/third_party/gmock',
        'export PROTOBUF_DIR=$$(pwd)/$(GENDIR)/third_party/protobuf',
        'export GSASL_DIR=$$(pwd)/$(GENDIR)/third_party/gsasl',
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t libhdfs3.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'tar xfz ' + package_file,
        'cd libhdfs3',
        'mkdir build',
        'cd build',
        '../bootstrap --prefix=$$INSTALL_DIR --dependency=$$GMOCK_DIR:$$GTEST_DIR:$$PROTOBUF_DIR:$$GSASL_DIR',
        'make install',
        'rm -rf $$INSTALL_DIR/lib/*.dylib $$INSTALL_DIR/lib/*.so',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "libhdfs3-cxx",
    srcs = ["empty.cc"] + lib_files,
    hdrs = include_files,
    includes = [
        "include",
    ],
    deps = [
      "//third_party/protobuf:protobuf-cxx",
      "//third_party/gsasl:gsasl-cxx",
    ],
    linkstatic = 1,
)

filegroup(
    name = "libhdfs3",
    srcs = [
        ":libhdfs3-cxx",
    ]
)
