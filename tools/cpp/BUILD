licenses(["notice"])

package(default_visibility = ["//visibility:public"])

action_listener(
    name = "compile_cpp",
    extra_actions = [
        ":checkstyle_cpp",
        ":cpp_cppcheck",
    ],
    mnemonics = [
        "CCompile",
        "CppCompile",
    ],
)

extra_action(
    name = "checkstyle_cpp",
    cmd = "$(execpath //tools/java/src/org/apache/bazel/checkstyle:checkstyle_cpp) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--cpplint_file $$(pwd)/$(execpath //third_party/python/cpplint)",
    requires_action_output = True,
    tools = [
        "//third_party/python/cpplint",
        "//tools/java/src/org/apache/bazel/checkstyle:checkstyle_cpp",
    ],
)

extra_action(
    name = "cpp_cppcheck",
    cmd = "$(execpath //tools/java/src/org/apache/bazel/cppcheck:cppcheck_cpp) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--cppcheck_file $$(pwd)/$(execpath //third_party/cppcheck:heron-cppcheck)",
    requires_action_output = True,
    tools = [
        "//third_party/cppcheck:heron-cppcheck",
        "//tools/java/src/org/apache/bazel/cppcheck:cppcheck_cpp",
    ],
)
