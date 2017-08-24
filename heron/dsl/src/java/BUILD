licenses(["notice"])

package(default_visibility = ["//visibility:public"])

dsl_deps_files = [
      "//heron/api/src/java:heron-api",
    ]

java_library(
    name = "dsl-java",
    srcs = glob(["com/twitter/heron/dsl/**/*.java"]),
    deps = dsl_deps_files,
)
