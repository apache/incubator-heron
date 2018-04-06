package(default_visibility = ["//visibility:public"])

heron_deps_files = [
    "//heron/api/src/java:api-java-low-level",
]

third_party_deps_files = [
    "//third_party/java:guava",
    "//third_party/java:kryo",
    "//third_party/java:logging",
]

all_deps = heron_deps_files + third_party_deps_files

java_binary(
    name = 'twitter-spout',
    srcs = glob(["com/twitter/heron/twitter/**/*.java"]),
    deps = all_deps + [
        "//third_party/java:twitter4j",
    ],
)
