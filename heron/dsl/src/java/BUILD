licenses(["notice"])

package(default_visibility = ["//visibility:public"])

dsl_shaded_deps_files = [
      "//heron/api/src/java:heron-api",
      "//heron/common/src/java:basics-java",
    ]

dsl_unshaded_deps_files = [
    "//heron/api/src/java:api-java",
    "//heron/api/src/java:classification",
    "//heron/common/src/java:basics-java",
]

java_library(
    name = "dsl-java",
    srcs = glob(["com/twitter/heron/dsl/**/*.java"]),
    deps = dsl_shaded_deps_files,
)

java_library(
    name = "dsl-unshaded-java",
    srcs = glob(["com/twitter/heron/dsl/**/*.java"]),
    deps = dsl_unshaded_deps_files,
)
