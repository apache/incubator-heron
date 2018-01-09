licenses(["notice"])

package(default_visibility = ["//visibility:public"])

eco_deps = [
"@commons_cli_commons_cli//jar",
"@org_yaml_snakeyaml//jar",
"//third_party/java:logging",
"//storm-compatibility/src/java:storm-compatibility-java",
"//heron/api/src/java:api-java-low-level",
"//heron/common/src/java:basics-java",
]


java_binary(
    name="eco-core-shaded",
    srcs = glob(["com/twitter/heron/eco/**/*.java"]),
    deps = eco_deps,
    create_executable = 0,

)

java_library(
    name = "eco-java",
    srcs = glob(["com/twitter/heron/eco/**/*.java"]),
    deps = eco_deps
)

genrule(
    name = "eco-core",
    srcs = [":eco-core-shaded_deploy.jar"],
    outs = ["eco-core.jar"],
    cmd  = "cp $< $@",
)
