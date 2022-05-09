load("@rules_java//java:defs.bzl", "java_binary", "java_library")

package(default_visibility = ["//visibility:public"])

dlog_deps = [
    "//third_party/java:dlog-java",
    "@maven//:org_slf4j_log4j_over_slf4j",
    "@maven//:io_netty_netty_all",
]

java_library(
    name = "dlog-lib",
    srcs = glob(["**/dlog/DL*.java"]),
    deps = dlog_deps,
)

java_binary(
    name = "dlog-util-unshaded",
    srcs = glob(["**/dlog/*.java"]),
    main_class = "org.apache.heron.dlog.Util",
    deps = dlog_deps + ["//heron/common/src/java:basics-java"],
)

genrule(
    name = "dlog-util",
    srcs = [":dlog-util-unshaded_deploy.jar"],
    outs = ["dlog-util.jar"],
    cmd = "cp $< $@",
)
