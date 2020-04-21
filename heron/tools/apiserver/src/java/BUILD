load("@rules_java//java:defs.bzl", "java_binary")

package(default_visibility = ["//visibility:public"])

api_deps_files = [
    "//heron/spi/src/java:heron-spi",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:utils-java",
    "//heron/api/src/java:api-java-low-level",
]

scheduler_deps_files = [
    "//heron/scheduler-core/src/java:scheduler-java",
    "//heron/schedulers/src/java:local-scheduler-java",
    "//heron/schedulers/src/java:kubernetes-scheduler-java",
    "//heron/schedulers/src/java:marathon-scheduler-java",
    "//heron/schedulers/src/java:mesos-scheduler-java",
    "//heron/schedulers/src/java:yarn-scheduler-java",
    "//heron/schedulers/src/java:slurm-scheduler-java",
    "//heron/schedulers/src/java:nomad-scheduler-java",
]

packing_deps_files = [
    "//heron/packing/src/java:roundrobin-packing",
    "//heron/packing/src/java:binpacking-packing",
    "//heron/packing/src/java:builder",
]

uploader_deps_files = [
    "//heron/uploaders/src/java:localfs-uploader-java",
    "//heron/uploaders/src/java:gcs-uploader-java",
    "//heron/uploaders/src/java:s3-uploader-java",
    "//heron/uploaders/src/java:dlog-uploader-java",
    "//heron/uploaders/src/java:http-uploader-java",
]

state_manager_deps_files = [
    "//heron/statemgrs/src/java:statemgrs-java",
]

apiserver_deps_files = \
    api_deps_files + \
    heron_java_proto_files() + \
    state_manager_deps_files + \
    scheduler_deps_files + \
    packing_deps_files + \
    uploader_deps_files + [
        "//third_party/java:cli",
        "//third_party/java:jetty-jersey-java",
        "//third_party/java:commons-compress",
        "//third_party/java:jackson",
        "//third_party/java:logging",
        "@maven//:javax_activation_activation",
        "@maven//:javax_annotation_javax_annotation_api",
        "@maven//:javax_xml_bind_jaxb_api",
        "@maven//:org_yaml_snakeyaml",
    ]

java_binary(
    name = "heron-apiserver-unshaded",
    srcs = glob(["**/apiserver/**/*.java"]),
    main_class = "org.apache.heron.apiserver.Runtime",
    deps = apiserver_deps_files,
)

genrule(
    name = "heron-apiserver",
    srcs = [":heron-apiserver-unshaded_deploy.jar"],
    outs = ["heron-apiserver.jar"],
    cmd = "cp $< $@",
)
