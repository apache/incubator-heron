licenses(["notice"])
package(default_visibility = ["//visibility:public"])

common_deps_files = [
      "@commons_cli_commons_cli//jar",
      "//heron/common/src/java:utils-java"
    ]
    
java_binary(
    name="eco-core",
    srcs = glob(["com/twitter/heron/eco/**/*.java"]),
    deps = common_deps_files
 )