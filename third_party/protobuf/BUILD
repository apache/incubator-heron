licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "protobuf"
package_version = "2.5.0"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version
package_patch = package_name + "-" + package_version + ".patch"

bin_files = [
    "bin/protoc",
]

include_files = [
    "include/google/protobuf/extension_set.h",
    "include/google/protobuf/message_lite.h",
    "include/google/protobuf/unknown_field_set.h",
    "include/google/protobuf/descriptor.h",
    "include/google/protobuf/generated_enum_reflection.h",
    "include/google/protobuf/reflection_ops.h",
    "include/google/protobuf/wire_format.h",
    "include/google/protobuf/descriptor.pb.h",
    "include/google/protobuf/generated_message_reflection.h",
    "include/google/protobuf/repeated_field.h",
    "include/google/protobuf/wire_format_lite.h",
    "include/google/protobuf/descriptor.proto",
    "include/google/protobuf/generated_message_util.h",
    "include/google/protobuf/service.h",
    "include/google/protobuf/wire_format_lite_inl.h",
    "include/google/protobuf/descriptor_database.h",
    "include/google/protobuf/dynamic_message.h",
    "include/google/protobuf/message.h",
    "include/google/protobuf/text_format.h",

    "include/google/protobuf/compiler/code_generator.h",
    "include/google/protobuf/compiler/command_line_interface.h",
    "include/google/protobuf/compiler/importer.h",
    "include/google/protobuf/compiler/parser.h",
    "include/google/protobuf/compiler/plugin.h",
    "include/google/protobuf/compiler/plugin.pb.h",
    "include/google/protobuf/compiler/plugin.proto",

    "include/google/protobuf/compiler/cpp/cpp_generator.h",
    "include/google/protobuf/compiler/java/java_generator.h",
    "include/google/protobuf/compiler/python/python_generator.h",

    "include/google/protobuf/io/coded_stream.h",
    "include/google/protobuf/io/gzip_stream.h",
    "include/google/protobuf/io/printer.h",
    "include/google/protobuf/io/tokenizer.h",
    "include/google/protobuf/io/zero_copy_stream.h",
    "include/google/protobuf/io/zero_copy_stream_impl.h",
    "include/google/protobuf/io/zero_copy_stream_impl_lite.h",

    "include/google/protobuf/stubs/atomicops.h",
    "include/google/protobuf/stubs/atomicops_internals_mips_gcc.h",
    "include/google/protobuf/stubs/once.h",
    "include/google/protobuf/stubs/atomicops_internals_arm_gcc.h",
    "include/google/protobuf/stubs/atomicops_internals_pnacl.h",
    "include/google/protobuf/stubs/platform_macros.h",
    "include/google/protobuf/stubs/atomicops_internals_arm_qnx.h",
    "include/google/protobuf/stubs/atomicops_internals_x86_gcc.h",
    "include/google/protobuf/stubs/template_util.h",
    "include/google/protobuf/stubs/atomicops_internals_atomicword_compat.h",
    "include/google/protobuf/stubs/atomicops_internals_x86_msvc.h",
    "include/google/protobuf/stubs/type_traits.h",
    "include/google/protobuf/stubs/atomicops_internals_macosx.h",
    "include/google/protobuf/stubs/common.h",
]

lib_files = [
    "lib/libprotobuf-lite.a",
    "lib/libprotobuf.a",
    "lib/libprotoc.a",
]

genrule(
    name = "protobuf-srcs",
    srcs = [package_file, package_patch],
    outs = bin_files + include_files + lib_files,
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t protobuf.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'tar xfz ' + package_file,
        'patch -d ' + package_dir + ' -p1 < ' + package_patch,
        'cd ' + package_dir,
        './configure --prefix=$$INSTALL_DIR --enable-shared=no',
        'make install',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "protobuf-cxx-lite",
    srcs = [
        "empty.cc",
        "lib/libprotobuf-lite.a",
    ],
    hdrs = [
        "include/google/protobuf/message_lite.h",
    ],
    includes = ["include"],
    linkstatic = 1,
)

cc_library(
    name = "protobuf-cxx",
    srcs = ["empty.cc"] + lib_files,
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

cc_library(
    name = "protoc-cxx",
    srcs = [
        "empty.cc",
        "lib/libprotoc.a",
    ],
    deps = [":protobuf-cxx"],
    linkstatic = 1,
)

filegroup(
    name = "protoc",
    srcs = ["bin/protoc"],
)

filegroup(
    name = "protobuf-files",
    srcs = bin_files + include_files + lib_files,
)
