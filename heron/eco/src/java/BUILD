licenses(["notice"])

package(default_visibility = ["//visibility:public"])

    
java_binary(
    name="eco-core-shaded",
    srcs = glob(["com/twitter/heron/eco/**/*.java"]),
    deps = [
     "@commons_cli_commons_cli//jar",
     "//heron/common/src/java:utils-java",
    ],
    create_executable = 0,
    
)

genrule(
    name = "eco-core",
    srcs = [":eco-core-shaded_deploy.jar"],
    outs = ["eco-core.jar"],
    cmd  = "cp $< $@",
)
 

