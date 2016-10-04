package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = \
    heron_java_proto_files() + [
        "//heron/spi/src/java:common-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/spi/src/java:network-utils-java",
        "@com_google_guava_guava//jar",
    ]

localfs_deps_files = \
    common_deps_files + [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:basics-java",
        "//heron/spi/src/java:heron-spi",
    ]
    
zookeeper_deps_files = \
    localfs_deps_files + [
        "@org_apache_curator_curator_client//jar",
        "@org_apache_curator_curator_framework//jar",
        "@org_apache_curator_curator_recipes//jar",
        "@org_apache_zookeeper_zookeeper//jar",
        "//third_party/java:logging"
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
    cmd  = "cp $< $@",
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
    resources = glob(["**/localfs/**/*.yaml"]),
    deps = localfs_deps_files,
    main_class="com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager"
)

genrule(
    name = "heron-localfs-statemgr",
    srcs = [":localfs-statemgr-unshaded_deploy.jar"],
    outs = ["heron-localfs-statemgr.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name = "zookeeper-statemgr-java",
    srcs = glob(["**/zookeeper/**/*.java"]) + glob(["**/FileSystemStateManager.java"]),
    deps = zookeeper_deps_files, 
)

java_binary(
    name = "zookeeper-statemgr-unshaded",
    srcs = glob(["**/zookeeper/**/*.java"]) + glob(["**/FileSystemStateManager.java"]),
    deps = zookeeper_deps_files,
    main_class="com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager"
)

genrule(
    name = "heron-zookeeper-statemgr",
    srcs = [":zookeeper-statemgr-unshaded_deploy.jar"],
    outs = ["heron-zookeeper-statemgr.jar"],
    cmd  = "cp $< $@",
)
