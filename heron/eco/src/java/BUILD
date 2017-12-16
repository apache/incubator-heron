licenses(["notice"])

package(default_visibility = ["//visibility:public"])

    
java_binary(
    name="eco-core-shaded",
    srcs = glob(["com/twitter/heron/eco/**/*.java"]),
    deps = [
     "@commons_cli_commons_cli//jar",
     "@org_yaml_snakeyaml//jar",
     "//heron/common/src/java:utils-java",
     "//heron/common/src/java:basics-java",
     
    ],
    create_executable = 0,
    
)

genrule(
    name = "eco-core",
    srcs = [":eco-core-shaded_deploy.jar"],
    outs = ["eco-core.jar"],
    cmd  = "cp $< $@",
)
 

