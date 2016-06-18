load("pex_rules", "pex_library")

standard_proto_path = "heron/proto"

def _genproto_impl(ctx):
  proto_src_deps = [src.proto_src for src in ctx.attr.deps]
  inputs, outputs, arguments = [ctx.file.src] + proto_src_deps, [], ["--proto_path=."]
  for src in proto_src_deps:
    if src.path.startswith(standard_proto_path):
      arguments += ["--proto_path=" + standard_proto_path]
      break

  if ctx.attr.gen_cc:
    outputs += [ctx.outputs.cc_hdr, ctx.outputs.cc_src]
    arguments += ["--cpp_out=" + ctx.configuration.genfiles_dir.path]

  if ctx.attr.gen_java:
    if ctx.outputs.java_src.path.endswith(".srcjar"):
      srcjar = ctx.new_file(ctx.outputs.java_src.basename[:-6] + "jar")
    else:
      srcjar = ctx.outputs.java_src
    outputs += [srcjar]
    arguments += ["--java_out=" + srcjar.path]

  if ctx.attr.gen_py:
    outputs += [ctx.outputs.py_src]
    arguments += ["--python_out=" + ctx.configuration.genfiles_dir.path]

  ctx.action(
      mnemonic = "GenProto",
      inputs = inputs,
      outputs = outputs,
      arguments = arguments + [ctx.file.src.path],
      executable = ctx.executable._protoc)

  # This is required because protoc only understands .jar extensions, but Bazel
  # requires source JAR files end in .srcjar.
  if ctx.attr.gen_java and srcjar != ctx.outputs.java_src:
    ctx.action(
        mnemonic = "FixProtoSrcJar",
        inputs = [srcjar],
        outputs = [ctx.outputs.java_src],
        arguments = [srcjar.path, ctx.outputs.java_src.path],
        command = "cp $1 $2")

    # Fixup the resulting outputs to keep the source-only .jar out of the result.
    outputs += [ctx.outputs.java_src]
    outputs = [e for e in outputs if e != srcjar]

  return struct(files=set(outputs),
                proto_src=ctx.file.src)

_genproto_attrs = {
    "src": attr.label(
        allow_files = FileType([".proto"]),
        single_file = True,
    ),
    "deps": attr.label_list(
        allow_files = False,
        providers = ["proto_src"],
    ),
    "_protoc": attr.label(
        default = Label("//third_party/protobuf:protoc"),
        executable = True,
    ),
    "gen_cc": attr.bool(),
    "gen_java": attr.bool(),
    "gen_py": attr.bool(),
}

def _genproto_outputs(attrs):
  outputs = {}
  if attrs.gen_cc:
    outputs += {
        "cc_hdr": "%{src}.pb.h",
        "cc_src": "%{src}.pb.cc"
    }
  if attrs.gen_java:
    outputs += {
        "java_src": "%{src}.srcjar",
    }
  if attrs.gen_py:
    outputs += {
        "py_src": "%{src}_pb2.py"
    }
  return outputs

genproto = rule(
    _genproto_impl,
    attrs = _genproto_attrs,
    output_to_genfiles = True,
    outputs = _genproto_outputs,
)

def proto_library(name, src=None, deps=[], visibility=None,
                  gen_java=False, gen_cc=False, gen_py=False):
  if not src:
    if name.endswith("_proto"):
      src = name[:-6] + ".proto"
    else:
      src = name + ".proto"

  proto_pkg = genproto(name=name,
                       src=src,
                       deps=deps,
                       gen_java=gen_java,
                       gen_cc=gen_cc,
                       gen_py=gen_py)

  # TODO(shahms): These should probably not be separate libraries, but
  # allowing upstream *_library and *_binary targets to depend on the
  # proto_library() directly is a challenge.  We'd also need a different
  # workaround for the non-generated any.pb.{h,cc} from the upstream protocol
  # buffer library.
  if gen_java:
    java_deps = ["@com_google_protobuf_protobuf_java//jar"]
    for dep in deps:
      java_deps += [dep + "_java"]
    native.java_library(
        name  = name + "_java",
        srcs = [proto_pkg.label()],
        deps = java_deps,
        visibility = visibility,
    )

  if gen_cc:
    cc_deps = ["//third_party/protobuf:protobuf-cxx"]
    for dep in deps:
      cc_deps += [dep + "_cc"]
    native.cc_library(
        name = name + "_cc",
        visibility = visibility,
        hdrs = [proto_pkg.label()],
        srcs = [proto_pkg.label()],
        defines = ["GOOGLE_PROTOBUF_NO_RTTI"],
        deps = cc_deps,
    )

  if gen_py:
    py_deps = []
    for dep in deps:
      py_deps += [dep + "_py"]
    pex_library(
        name = name + "_py",
        visibility = visibility,
        srcs = [proto_pkg.label()],
        deps = py_deps,
    )

