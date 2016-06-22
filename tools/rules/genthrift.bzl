def thrift_package_impl(ctx):
  return struct(thrift_src = ctx.file.src)

genthrift_base_attrs = {
    "src": attr.label(
        allow_files = FileType([".thrift"]),
        single_file = True,
    ),
    "deps": attr.label_list(
        allow_files = False,
        providers = ["thrift_src"],
    ),
}

thrift_package = rule(
    thrift_package_impl,
    attrs = genthrift_base_attrs,
)

def genthrift_java_impl(ctx):
  src = ctx.file.src
  thrift = ctx.file._thrift

  srcjar = ctx.new_file(ctx.configuration.genfiles_dir, ctx.label.name + ".srcjar")
  java_srcs = srcjar.path + ".srcs"

  inputs = [src, thrift]
  java_cmd = '\n'.join([
      "set -e",
      "rm -rf " + java_srcs,
      "mkdir " + java_srcs,
      thrift.path + " -r --gen java -o " + java_srcs + " " + src.path,
      "jar cMf " + srcjar.path + " -C " + java_srcs + " .",
      "rm -rf " + java_srcs,
  ])
  ctx.action(
      inputs = inputs,
      outputs = [srcjar],
      mnemonic = 'ThriftJava',
      command = java_cmd,
      use_default_shell_env = True)

  return struct(files = set([srcjar]))

genthrift_java = rule(
    genthrift_java_impl,
    attrs = genthrift_base_attrs + {
        "_thrift": attr.label(
            default = Label("//third_party/thrift:thrift"),
            allow_files = True,
            single_file = True,
        ),
    },
)

def thrift_library(name, src=None, deps=[], visibility=None,
                  gen_java=False, gen_cc=False):
  if not src:
    if name.endswith("_thrift"):
      src = name[:-7]+".thrift"
    else:
      src = name+".thrift"
  thrift_package(name=name, src=src, deps=deps)

  if gen_java:
    genthrift_java(
        name = name+"_java_src",
        src = src,
        deps = deps,
        visibility = ["//visibility:private"],
    )
    java_deps = [
        "@org_apache_thrift_libthrift//jar",
        "//third_party/java:logging",
    ]
    for dep in deps:
      java_deps += [dep+"_java"]
    native.java_library(
        name  = name+"_java",
        srcs = [name+"_java_src"],
        deps = java_deps,
        visibility = visibility,
        javacopts = [
          "-Xlint:-cast",
          "-Xlint:-rawtypes",
          "-Xlint:-serial",
          "-Xlint:-static",
          "-Xlint:-unchecked",
        ],
    )
