package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")

java_library(
    name = 'config-java',
    srcs = glob([
        "**/config/**/*.java",
    ]),
    deps = [
        "//3rdparty/snakeyaml:snakeyaml-java",
    ],
)

