load("@rules_cc//cc:defs.bzl", "cc_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

genrule(
    name = "zookeeper-srcs",
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
    cmd = select({
        ":darwin": "\n".join([
            "export INSTALL_DIR=$$(pwd)/$(@D)",
            "export TMP_DIR=$$(mktemp -d -t zookeeper.XXXXX)",
            "export CXXFLAGS=-Wno-error",
            "export CFLAGS=-Wno-error",
            "mkdir -p $$TMP_DIR",
            "cp -R $$(pwd)/external/org_apache_zookeeper/* $$TMP_DIR",
            "cd $$TMP_DIR/zookeeper-client/zookeeper-client-c",
            "./configure --prefix=$$INSTALL_DIR --enable-shared=no",
            "make install",
            "rm -rf $$TMP_DIR",
        ]),
        "//conditions:default": "\n".join([
            "export INSTALL_DIR=$$(pwd)/$(@D)",
            "export TMP_DIR=$$(mktemp -d -t zookeeper.XXXXX)",
            "export CXXFLAGS=-Wno-error",
            "export CFLAGS=-Wno-error",
            "mkdir -p $$TMP_DIR",
            "cp -R $$(pwd)/external/org_apache_zookeeper/* $$TMP_DIR",
            "cd $$TMP_DIR/zookeeper-client/zookeeper-client-c",
            "autoreconf -if",
            "./configure --prefix=$$INSTALL_DIR --enable-shared=no",
            "make install",
            "rm -rf $$TMP_DIR",
        ]),
    }),
)

cc_library(
    name = "zookeeper_st-cxx",
    srcs = [
        "include/zookeeper/proto.h",
        "include/zookeeper/recordio.h",
        "include/zookeeper/zookeeper.jute.h",
        "include/zookeeper/zookeeper_log.h",
        "include/zookeeper/zookeeper_version.h",
        "lib/libzookeeper_st.a",
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
    name = "zookeeper",
    srcs = [
        "include/zookeeper/proto.h",
        "include/zookeeper/recordio.h",
        "include/zookeeper/zookeeper.jute.h",
        "include/zookeeper/zookeeper_log.h",
        "include/zookeeper/zookeeper_version.h",
        "lib/libzookeeper_mt.a",
    ],
    hdrs = [
        "include/zookeeper/zookeeper.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)

config_setting(
    name = "darwin",
    values = {
        "cpu": "darwin",
    },
    visibility = ["//visibility:public"],
)
