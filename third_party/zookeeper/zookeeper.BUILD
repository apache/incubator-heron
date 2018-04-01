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

    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t zookeeper.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $$(pwd)/external/org_apache_zookeeper/* $$TMP_DIR",
        "cd $$TMP_DIR/src/c",
        "./configure --prefix=$$INSTALL_DIR --enable-shared=no",
        "make install",
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "zookeeper_st-cxx",
    srcs = [
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
    name = "zookeeper",
    srcs = [
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
