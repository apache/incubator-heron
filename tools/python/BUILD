licenses(["notice"])

load("//pex:pex_rules.bzl", "pex_binary")

package(default_visibility = ["//visibility:public"])

action_listener(
    name = "compile_python",
    extra_actions = [":checkstyle_python"],
    mnemonics = ["PexPython"],
)

extra_action(
    name = "checkstyle_python",
    cmd = "$(location //tools/java/src/com/twitter/bazel/checkstyle:checkstyle_python) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--pylint_file $$(pwd)/$(location //third_party/python/pylint)",
    requires_action_output = True,
    tools = [
        "//third_party/python/pylint",
        "//tools/java/src/org/apache/bazel/checkstyle:checkstyle_python",
    ],
)
