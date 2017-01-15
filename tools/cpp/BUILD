licenses(["notice"])

package(default_visibility = ["//visibility:public"])

action_listener(
    name = "compile_cpp",
    mnemonics = ["CCompile", "CppCompile"],
    extra_actions = [":checkstyle_cpp"],
)

extra_action(
    name = "checkstyle_cpp",
    tools = [
        "//third_party/python/cpplint",
        "//tools/java/src/com/twitter/bazel/checkstyle:checkstyle_cpp",
    ],
    requires_action_output = True,
    cmd = "$(location //tools/java/src/com/twitter/bazel/checkstyle:checkstyle_cpp) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--cpplint_file $$(pwd)/$(location //third_party/python/cpplint)"
)