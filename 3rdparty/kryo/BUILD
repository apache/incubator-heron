licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_import(
    name = "minlog-java",
    jars = ["minlog-1.3.0.jar"],
)

java_import(
    name = "objenesis-java",
    jars = ["objenesis-2.1.jar"],
)

java_import(
    name = "kryo-java",
    deps = [
        "//3rdparty/kryo:minlog-java",
        "//3rdparty/kryo:objenesis-java"
    ],
    jars = ["kryo-3.0.3.jar"],
)
