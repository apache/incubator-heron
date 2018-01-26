licenses(["notice"])

package(default_visibility = ["//visibility:public"])

scala_library(
    name = "api-scala",
    srcs = glob(["com/twitter/heron/streamlet/scala/**/*.scala"]),
    deps = [
            "//heron/api/src/java:api-java-low-level",
            "//heron/api/src/java:api-java"
        ]
)