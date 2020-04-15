#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

load("@rules_java//java:defs.bzl", "java_library")
load("@rules_cc//cc:defs.bzl", "cc_library")
load("pex_rules", "pex_library")

def proto_package_impl(ctx):
    return struct(proto_src = ctx.file.src)

genproto_base_attrs = {
    "src": attr.label(
        allow_files = [".proto"],
        allow_single_file = True,
    ),
    "deps": attr.label_list(
        allow_files = False,
        providers = ["proto_src"],
    ),
}

proto_package = rule(
    proto_package_impl,
    attrs = genproto_base_attrs,
)

def genproto_java_impl(ctx):
    src = ctx.file.src
    protoc = ctx.file._protoc

    srcjar = ctx.actions.declare_file(ctx.configuration.genfiles_dir, ctx.label.name + ".srcjar")
    java_srcs = srcjar.path + ".srcs"

    inputs = [src, protoc]
    java_cmd = "\n".join([
        "set -e",
        "rm -rf " + java_srcs,
        "mkdir " + java_srcs,
        protoc.path + " -I heron/proto --java_out=" + java_srcs + " " + src.path,
        "jar cMf " + srcjar.path + " -C " + java_srcs + " .",
        "rm -rf " + java_srcs,
    ])
    ctx.actions.run(
        inputs = inputs,
        outputs = [srcjar],
        mnemonic = "ProtocJava",
        command = java_cmd,
        use_default_shell_env = True,
    )

    return struct(files = set([srcjar]))

genproto_java = rule(
    genproto_java_impl,
    attrs = genproto_base_attrs.update({
        "_protoc": attr.label(
            default = Label("//third_party/protobuf:protoc"),
            allow_files = True,
            allow_single_file = True,
        ),
    }),
)

def proto_library(
        name,
        src = None,
        includes = [],
        deps = [],
        visibility = None,
        gen_java = False,
        gen_cc = False,
        gen_py = False):
    if not src:
        if name.endswith("_proto"):
            src = name[:-6] + ".proto"
        else:
            src = name + ".proto"
    proto_package(name = name, src = src, deps = deps)

    if gen_java:
        genproto_java(
            name = name + "_java_src",
            src = src,
            deps = deps,
            visibility = ["//visibility:private"],
        )
        java_deps = ["@com_google_protobuf//:protobuf_java"]
        for dep in deps:
            java_deps.append(dep + "_java")
        java_library(
            name = name + "_java",
            srcs = [name + "_java_src"],
            deps = java_deps,
            visibility = visibility,
        )

    if not includes:
        proto_include_paths = ""
    else:
        proto_include_paths = "".join(["-I " + incl for incl in includes])

    if gen_cc:
        # We'll guess that the repository is set up such that a .proto in
        # //foo/bar has the package foo.bar. `location` is substituted with the
        # relative path to its label from the workspace root.
        proto_path = "$(location %s)" % src
        proto_hdr = src[:-6] + ".pb.h"
        proto_src = src[:-6] + ".pb.cc"
        proto_srcgen_rule = name + "_cc_src"
        proto_lib = name + "_cc"
        protoc = "//third_party/protobuf:protoc"
        if not includes:
            proto_cmd = "$(location %s) --cpp_out=$(@D) %s" % (protoc, proto_path)
        else:
            proto_cmd = "$(location %s) %s --cpp_out=$(@D) %s" % (protoc, proto_include_paths, proto_path)

        cc_deps = ["//third_party/protobuf:protobuf-cxx"]
        proto_deps = [src, protoc]
        for dep in deps:
            cc_deps.append(dep + "_cc")
            proto_deps.append(dep)
        native.genrule(
            name = proto_srcgen_rule,
            visibility = visibility,
            outs = [proto_hdr, proto_src],
            srcs = proto_deps,
            cmd = proto_cmd,
        )
        cc_library(
            name = proto_lib,
            visibility = visibility,
            hdrs = [proto_hdr],
            srcs = [":" + proto_srcgen_rule],
            defines = ["GOOGLE_PROTOBUF_NO_RTTI"],
            deps = cc_deps,
            linkstatic = 1,
        )

    if gen_py:
        # We'll guess that the repository is set up such that a .proto in
        # //foo/bar has the package foo.bar. `location` is substituted with the
        # relative path to its label from the workspace root.
        proto_path = "$(location %s)" % src
        proto_src = src[:-6] + "_pb2.py"
        proto_srcgen_rule = name + "_py_src"
        proto_lib = name + "_py"
        protoc = "//third_party/protobuf:protoc"
        if not includes:
            proto_cmd = "$(location %s) --python_out=$(@D) %s" % (protoc, proto_path)
        else:
            proto_cmd = "$(location %s) %s --python_out=$(@D) %s" % (protoc, proto_include_paths, proto_path)
        py_deps = []
        proto_deps = [src, protoc]
        for dep in deps:
            py_deps.append(dep + "_py")
            proto_deps.append(dep)
        native.genrule(
            name = proto_srcgen_rule,
            visibility = visibility,
            outs = [proto_src],
            srcs = proto_deps,
            cmd = proto_cmd,
        )
        pex_library(
            name = proto_lib,
            visibility = visibility,
            srcs = [proto_src],
            deps = py_deps,
        )
