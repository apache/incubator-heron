licenses(["notice"])

package(default_visibility = ["//visibility:public"])

# Google Test including Google Mock 
cc_library(
    name = "gtest",
    srcs = glob(
        include = [
            "googletest/src/*.cc",
            "googletest/src/*.h",
            "googletest/include/gtest/**/*.h",
            "googlemock/src/*.cc",
            "googlemock/include/gmock/**/*.h",
        ],
        exclude = [
            "googletest/src/gtest-all.cc",
            "googletest/src/gtest_main.cc",
            "googlemock/src/gmock-all.cc",
            "googlemock/src/gmock_main.cc",
        ],
    ),
    hdrs =glob([
        "googletest/include/gtest/*.h",
        "googlemock/include/gmock/*.h",
    ]),
    includes = [
         "googlemock",
         "googlemock/include",
         "googletest",
         "googletest/include",
    ],
)

cc_library(
    name = "gtest_main",
    srcs = [
        "googlemock/src/gmock_main.cc",
    ],
    deps = ["//:gtest"],
)
