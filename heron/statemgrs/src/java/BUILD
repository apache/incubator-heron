load("@rules_java//java:defs.bzl", "java_binary", "java_library")

package(default_visibility = ["//visibility:public"])

common_deps_files = \
    heron_java_proto_files() + [
        "//heron/spi/src/java:common-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/spi/src/java:network-utils-java",
        "@maven//:com_google_guava_guava",
    ]

localfs_deps_files = \
    common_deps_files + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/spi/src/java:heron-spi",
    ]

zookeeper_deps_files = \
    localfs_deps_files + [
        "@maven//:org_apache_curator_curator_client",
        "@maven//:org_apache_curator_curator_framework",
        "@maven//:org_apache_curator_curator_recipes",
        "@maven//:org_apache_zookeeper_zookeeper",
        "//third_party/java:logging",
    ]

java_library(
    name = "statemgrs-java",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = zookeeper_deps_files,
)

java_library(
    name = "null-statemgr-java",
    srcs = glob(["**/NullStateManager.java"]),
    deps = common_deps_files,
)

java_binary(
    name = "null-statemgr-unshaded",
    srcs = glob(["**/NullStateManager.java"]),
    deps = common_deps_files,
)

genrule(
    name = "heron-null-statemgr",
    srcs = [":null-statemgr-unshaded_deploy.jar"],
    outs = ["heron-null-statemgr.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "localfs-statemgr-java",
    srcs = glob(["**/FileSystemStateManager.java"]) + glob(["**/localfs/**/*.java"]),
    resources = glob(["**/localfs/**/*.yaml"]),
    deps = localfs_deps_files,
)

java_binary(
    name = "localfs-statemgr-unshaded",
    srcs = glob(["**/FileSystemStateManager.java"]) + glob(["**/localfs/**/*.java"]),
    main_class = "org.apache.heron.statemgr.localfs.LocalFileSystemStateManager",
    resources = glob(["**/localfs/**/*.yaml"]),
    deps = localfs_deps_files,
)

genrule(
    name = "heron-localfs-statemgr",
    srcs = [":localfs-statemgr-unshaded_deploy.jar"],
    outs = ["heron-localfs-statemgr.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "zookeeper-statemgr-java",
    srcs = glob(["**/zookeeper/**/*.java"]) + glob(["**/FileSystemStateManager.java"]),
    deps = zookeeper_deps_files,
)

java_binary(
    name = "zookeeper-statemgr-unshaded",
    srcs = glob(["**/zookeeper/**/*.java"]) + glob(["**/FileSystemStateManager.java"]),
    main_class = "org.apache.heron.statemgr.zookeeper.curator.CuratorStateManager",
    deps = zookeeper_deps_files,
)

genrule(
    name = "heron-zookeeper-statemgr",
    srcs = [":zookeeper-statemgr-unshaded_deploy.jar"],
    outs = ["heron-zookeeper-statemgr.jar"],
    cmd = "cp $< $@",
)
