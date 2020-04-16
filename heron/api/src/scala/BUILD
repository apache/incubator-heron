licenses(["notice"])

package(default_visibility = ["//visibility:public"])

scala_library(
    name = "api-scala",
    srcs = glob(["org/apache/heron/streamlet/scala/**/*.scala"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
    ],
)
