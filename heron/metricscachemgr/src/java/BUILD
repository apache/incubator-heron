licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

metricscachemgr_deps_files = heron_java_proto_files() + [
           "//heron/spi/src/java:common-spi-java",
           "//heron/spi/src/java:metricsmgr-spi-java",
           "//heron/spi/src/java:utils-spi-java",
           "//heron/spi/src/java:statemgr-spi-java",
           "//heron/spi/src/java:packing-spi-java",
           "//heron/api/src/java:api-java",
           "//heron/common/src/java:common-java",
           "//heron/scheduler-core/src/java:scheduler-java",
           "//heron/statemgrs/src/java:localfs-statemgr-java",
           "//heron/metricsmgr/src/thrift:thrift_scribe_java",
           "//heron/metricsmgr/src/java:metricsmgr-java",
           "//third_party/java:cli",
           "//third_party/java:guava",
]

java_library(
    name = "metricscachemgr-java",
    srcs = glob(["**/*.java"]),
    deps = metricscachemgr_deps_files,
)

java_binary(
    name = "metricscachemgr-unshaded",
    srcs = glob(["**/*.java"]),
    deps = metricscachemgr_deps_files,
)

genrule(
    name = "heron-metricscachemgr",
    srcs = [":metricscachemgr-unshaded_deploy.jar"],
    outs = ["heron-metricscachemgr.jar"],
    cmd  = "cp $< $@",
)

java_binary(
    name = "metricscache-queryclient-unshaded",
    srcs = glob(["**/*.java"]),
    deps = metricscachemgr_deps_files,
    main_class="com.twitter.heron.metricscachemgr.MetricsCacheManagerHttpServer"
)
