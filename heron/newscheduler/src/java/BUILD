package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
   "//3rdparty/guava:guava-java",
]

spi_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:uploader-spi-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/spi/src/java:scheduler-spi-java",
    "//heron/spi/src/java:utils-spi-java",
]

scheduler_deps_files = \
    common_deps_files + \
    heron_java_proto_files() + \
    spi_deps_files + [
        "//heron/common/src/java:common-java",
    ]

java_library(
    name = 'scheduler-java',
    srcs = glob(
        ["**/*.java"],
    ),
    deps = scheduler_deps_files,
)

java_binary(
    name = 'scheduler',
    srcs = glob([
        "**/SubmitterMain.java",
    ]),
    main_class = "com.twitter.heron.scheduler.SubmitterMain",
    deps = [
        ":scheduler-java",
    ] + scheduler_deps_files,
)
