licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "zookeeper"
package_version = "3.4.5-macfix"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

genrule(
    name = "zookeeper-srcs",
    srcs = [
        package_file, 
    ],
    outs = [

        "bin/cli_mt",
        "bin/cli_st",
        "bin/load_gen",

        "include/zookeeper/proto.h",
        "include/zookeeper/recordio.h",
        "include/zookeeper/zookeeper.h",
        "include/zookeeper/zookeeper.jute.h",
        "include/zookeeper/zookeeper_log.h",
        "include/zookeeper/zookeeper_version.h",

        "lib/libzookeeper_mt.a",
        "lib/libzookeeper_st.a",
    ],

    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t zookeeper.XXXXX)",
        "echo $$INSTALL_DIR",
        "mkdir -p $$TMP_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$TMP_DIR",
        "tar xfz " + package_file,
        "cd " + package_dir + "/src/c",
        "./configure --prefix=$$INSTALL_DIR --enable-shared=no",
        "make install",
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "zookeeper_st-cxx",
    srcs = [
        "empty.cc",
        "lib/libzookeeper_st.a",
        "include/zookeeper/proto.h",
        "include/zookeeper/recordio.h",
        "include/zookeeper/zookeeper.jute.h",
        "include/zookeeper/zookeeper_log.h",
        "include/zookeeper/zookeeper_version.h",
    ],
    hdrs = [
        "include/zookeeper/zookeeper.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)

cc_library(
    name = "zookeeper-cxx",
    srcs = [
        "empty.cc",
        "lib/libzookeeper_mt.a",
        "include/zookeeper/proto.h",
        "include/zookeeper/recordio.h",
        "include/zookeeper/zookeeper.jute.h",
        "include/zookeeper/zookeeper_log.h",
        "include/zookeeper/zookeeper_version.h",
    ],
    hdrs = [
        "include/zookeeper/zookeeper.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)
