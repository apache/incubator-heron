package(default_visibility = ["//visibility:public"])

load("/tools/build_defs/pkg/pkg", "pkg_tar")
load("/tools/build_defs/pkg/pkg", "pkg_deb")

load("/tools/rules/heron_client", "heron_client_bin_files")
load("/tools/rules/heron_client", "heron_client_conf_files")
load("/tools/rules/heron_client", "heron_client_local_files")
load("/tools/rules/heron_client", "heron_client_lib_files")

sh_binary(
    name = "package-info-generator",
    srcs = ["package_info_generator.sh"],
)

genrule(
    name = "generate-package-info",
    outs = ["README.md"],
    cmd = "$(location :package-info-generator) $$(find . -name '*status*.txt') >$@",
    stamp = 1,
    tools = [":package-info-generator"],
)

genrule(
    name = "generate-launcher",
    srcs = [
        "template_bin.sh",
        ":README.md",
    ],
    outs = ["launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :README.md))"
        template="$$(cat $(location template_bin.sh))"
        echo "$${template//%release_info%/$${release_info}}" >$@
        """,
)

load("self_extract_binary", "self_extract_binary")

#self_extract_binary(
#    name = "install.sh",
#    bin = heron_client_bin_files(),
#    conf = heron_client_conf_files(),
#    local = heron_client_local_files(),
#    dist = ["//release:heron-core"],
#    lib = heron_client_lib_files(),
#    launcher = ":launcher_bin.sh",
#)

self_extract_binary(
    name = "install.sh",
    flatten_resources = [
        "//release:heron-client",
    ],
    launcher = ":launcher_bin.sh", 
)
