licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "protobuf"
package_version = "3.0.0"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

gmock_name = "gmock"
gmock_version = "1.7.0"

gmock_file = gmock_name + "-" + gmock_version + ".zip"
gmock_dir = gmock_name + "-" + gmock_version

genrule(
    name = "protobuf-srcs",
    srcs = [package_file, gmock_file],
    outs = [
        "bin/protoc",
   

        "include/google/protobuf/arena.h",
        "include/google/protobuf/arenastring.h",
        "include/google/protobuf/stubs/atomic_sequence_num.h",
        "include/google/protobuf/stubs/fastmem.h",
        "include/google/protobuf/stubs/port.h",
        "include/google/protobuf/stubs/macros.h",
        "include/google/protobuf/stubs/logging.h",
        "include/google/protobuf/stubs/scoped_ptr.h",
        "include/google/protobuf/stubs/mutex.h",
        "include/google/protobuf/stubs/callback.h",
        "include/google/protobuf/stubs/shared_ptr.h",
        "include/google/protobuf/stubs/casts.h",

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
        "include/google/protobuf/generated_enum_util.h",
        "include/google/protobuf/service.h",
        "include/google/protobuf/wire_format_lite_inl.h",
        "include/google/protobuf/descriptor_database.h",
        "include/google/protobuf/dynamic_message.h",
        "include/google/protobuf/message.h",
        "include/google/protobuf/text_format.h",
        "include/google/protobuf/metadata.h",

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

        "lib/libprotobuf-lite.a",
        "lib/libprotobuf.a",
        "lib/libprotoc.a",
    ],
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t protobuf.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$TMP_DIR",
        "tar xfz " + package_file,
        "cd " + package_dir,
        "unzip -q -o " + "../" + gmock_file,
        "mv " + gmock_dir + " gmock",
        "./autogen.sh",
        "./configure --prefix=$$INSTALL_DIR --enable-shared=no",
        "make install",
        "rm -rf $$TMP_DIR",
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
    srcs = [
        "include/google/protobuf/descriptor.pb.h",
        "include/google/protobuf/generated_enum_reflection.h",

        "include/google/protobuf/stubs/atomicops.h",
        "include/google/protobuf/stubs/atomicops_internals_macosx.h",
        "include/google/protobuf/stubs/atomicops_internals_x86_gcc.h",
        "include/google/protobuf/stubs/atomicops_internals_atomicword_compat.h",
        "include/google/protobuf/stubs/platform_macros.h",
        "include/google/protobuf/stubs/type_traits.h",
        "include/google/protobuf/stubs/template_util.h",

        "empty.cc",
        "lib/libprotobuf.a",
    ],
    hdrs = [
        "include/google/protobuf/arena.h",
        "include/google/protobuf/arenastring.h",
        "include/google/protobuf/stubs/atomic_sequence_num.h",
        "include/google/protobuf/stubs/common.h",
        "include/google/protobuf/stubs/fastmem.h",
        "include/google/protobuf/stubs/once.h",
        "include/google/protobuf/stubs/port.h",
        "include/google/protobuf/stubs/macros.h",
        "include/google/protobuf/stubs/logging.h",
        "include/google/protobuf/stubs/scoped_ptr.h",
        "include/google/protobuf/stubs/mutex.h",
        "include/google/protobuf/stubs/callback.h",
        "include/google/protobuf/stubs/shared_ptr.h",
        "include/google/protobuf/stubs/casts.h",

        "include/google/protobuf/descriptor.h",
        "include/google/protobuf/extension_set.h",
        "include/google/protobuf/generated_enum_util.h",
        "include/google/protobuf/generated_message_util.h",
        "include/google/protobuf/generated_message_reflection.h",
        "include/google/protobuf/message.h",
        "include/google/protobuf/message_lite.h",
        "include/google/protobuf/metadata.h",
        "include/google/protobuf/reflection_ops.h",
        "include/google/protobuf/repeated_field.h",
        "include/google/protobuf/unknown_field_set.h",
        "include/google/protobuf/wire_format.h",
        "include/google/protobuf/wire_format_lite.h",
        "include/google/protobuf/wire_format_lite_inl.h",

        "include/google/protobuf/io/coded_stream.h",
    ],
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
    srcs = ["bin/protoc"]
)
