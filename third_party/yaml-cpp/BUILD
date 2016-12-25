licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "yaml-cpp"
package_version = "noboost"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

include_files = [
    "include/yaml-cpp/yaml.h",

    "include/yaml-cpp/anchor.h",
    "include/yaml-cpp/binary.h",
    "include/yaml-cpp/dll.h",
    "include/yaml-cpp/emitterdef.h",
    "include/yaml-cpp/emitterstyle.h",
    "include/yaml-cpp/exceptions.h",
    "include/yaml-cpp/noncopyable.h",
    "include/yaml-cpp/parser.h",
    "include/yaml-cpp/emitfromevents.h",
    "include/yaml-cpp/emittermanip.h",
    "include/yaml-cpp/mark.h",
    "include/yaml-cpp/null.h",
    "include/yaml-cpp/stlemitter.h",
    "include/yaml-cpp/emitter.h",
    "include/yaml-cpp/eventhandler.h",
    "include/yaml-cpp/ostream_wrapper.h",
    "include/yaml-cpp/traits.h",

    "include/yaml-cpp/contrib/anchordict.h",
    "include/yaml-cpp/contrib/graphbuilder.h",

    "include/yaml-cpp/node/convert.h",
    "include/yaml-cpp/node/emit.h",
    "include/yaml-cpp/node/impl.h",
    "include/yaml-cpp/node/iterator.h",
    "include/yaml-cpp/node/node.h",
    "include/yaml-cpp/node/parse.h",
    "include/yaml-cpp/node/ptr.h",
    "include/yaml-cpp/node/type.h",

    "include/yaml-cpp/node/detail/bool_type.h",
    "include/yaml-cpp/node/detail/iterator.h",
    "include/yaml-cpp/node/detail/memory.h",
    "include/yaml-cpp/node/detail/node_data.h",
    "include/yaml-cpp/node/detail/node_ref.h",
    "include/yaml-cpp/node/detail/impl.h",
    "include/yaml-cpp/node/detail/iterator_fwd.h",
    "include/yaml-cpp/node/detail/node.h",
    "include/yaml-cpp/node/detail/node_iterator.h",
]

lib_files = [
    "lib/libyaml-cpp.a",
]

genrule(
    name = "yaml-cpp-srcs",
    srcs = [
        package_file, 
    ],
    outs = include_files + lib_files,
    cmd = "\n".join([
        'export WORKSPACE_ROOT=$$(pwd)',
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t yaml-cpp.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'tar xfz ' + package_file,
        'cd ' + package_dir,
        '$$WORKSPACE_ROOT/$(location //scripts/compile:env_exec) cmake -DCMAKE_INSTALL_PREFIX:PATH=$$INSTALL_DIR .',
        '$$WORKSPACE_ROOT/$(location //scripts/compile:env_exec) make install',
        'rm -rf $$TMP_DIR',
    ]),
    tools = [
        "//scripts/compile:env_exec",
    ],
)

cc_library(
    name = "yaml-cxx",
    srcs = [
        "empty.cc",
        "lib/libyaml-cpp.a",
    ],
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)
