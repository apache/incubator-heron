package(default_visibility = ["//visibility:public"])

# config_setting(
#     name = "osx_x86_64",
#     constraint_values = {
#         "@platforms//arch:x86_64",
#         "@platforms//os:osx",
#     },
#     visibility = ["//visibility:public"],
# )

# config_setting(
#     name = "osx_arm",
#     constraint_values = {
#         "@platforms//arch:armv6-m1",
#         "@platforms//os:osx",
#     },
#     visibility = ["//visibility:public"],
# )

config_setting(
    name = "linux_x86_64",
    constraint_values = {
        "@platforms//arch:x86_64",
        "@platforms//os:linux",
    },
    values = {
        "linkopt": "-lm",
        "linkopt": "-lpthread",
        "linkopt": "-lrt",
    }
    visibility = ["//visibility:public"],
)
