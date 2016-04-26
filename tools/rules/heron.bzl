################################################################################
# Rule implmentation for installing heron files
################################################################################
def install_heron_files_impl(ctx):

    # construct the name of destination directories
    dest_dir = ctx.configuration.bin_dir.path + "/" + ctx.attr.dir
    bin_dest_dir = ctx.attr.dir + "/bin"
    lib_dest_dir = ctx.attr.dir + "/lib"
    conf_dest_dir = ctx.attr.dir + "/conf"
    others_dest_dir = ctx.attr.dir

    bin_files    = ctx.files.bin if ctx.files.bin else []
    lib_files    = ctx.files.lib if ctx.files.lib else []
    conf_files   = ctx.files.conf if ctx.files.conf else []
    others_files = ctx.files.others if ctx.files.others else []
    
    mkdir_cmds = []
    cp_cmds = []
    outputs = []
    if bin_files:
        mkdir_cmds += ["rm -rf " + bin_dest_dir, "mkdir -p " + bin_dest_dir]
        for src in ctx.files.bin:
            file_out = ctx.new_file(ctx.configuration.bin_dir, bin_dest_dir + "/" + src.basename)
            cp_cmds += ["cp %s %s" % (src.path, file_out.path)]
            outputs += [file_out]

    if conf_files:
        mkdir_cmds += ["rm -rf " + conf_dest_dir, "mkdir -p " + conf_dest_dir]
        for src in ctx.files.conf:
            file_out = ctx.new_file(ctx.configuration.bin_dir, conf_dest_dir + "/" + src.basename)
            cp_cmds += ["cp %s %s" % (src.path, file_out.path)]
            outputs += [file_out]

    if lib_files:
        mkdir_cmds += ["rm -rf " + lib_dest_dir, "mkdir -p " + lib_dest_dir]
        for src in ctx.files.lib:
            file_out = ctx.new_file(ctx.configuration.bin_dir, lib_dest_dir + "/" + src.basename)
            cp_cmds += ["cp %s %s" % (src.path, file_out.path)]
            outputs += [file_out]

    if others_files:
        mkdir_cmds += ["rm -rf " + others_dest_dir, "mkdir -p " + others_dest_dir]
        for src in ctx.files.others:
            file_out = ctx.new_file(ctx.configuration.bin_dir, others_dest_dir + "/" + src.basename)
            cp_cmds += ["cp %s %s" % (src.path, file_out.path)]
            outputs += [file_out]

    inputs  = bin_files + conf_files + lib_files + others_files

    ctx.action(
        command = " && ".join(mkdir_cmds + cp_cmds),
        inputs  = inputs,
        outputs = outputs,
        mnemonic= "CopyFiles",
        use_default_shell_env = True)

    runfiles = list(set(inputs)) + list(set(outputs))
    return struct(runfiles = ctx.runfiles(files = runfiles))

################################################################################
# Rule for installing heron files in a given directory
#  - bin, for binary or executable files that go under bin subfolder
#  - conf, for configuration files that go under conf subfolder
#  - lib, for library files that go under lib subfolder
#  - other, for miscellaneous files that go under the directory 
################################################################################
install_heron_files = rule(
    install_heron_files_impl,
    attrs = {
        "dir": attr.string(
            mandatory = True),
        "bin": attr.label_list(
            allow_files=True),
        "conf": attr.label_list(
            allow_files=True),
        "lib": attr.label_list(
            allow_files=True),
        "others": attr.label_list(
            allow_files=True),
    }
)

################################################################################
# Convenience macro for grouping all Heron API files
################################################################################
def heron_api_files():
    return heron_api_bin_files() + heron_api_conf_files() + heron_api_lib_files()

def heron_api_bin_files():
    return []

def heron_api_conf_files():
    return []

def heron_api_lib_files():
    return [
        "//heron/api/src/java:api-java",
    ]

################################################################################
# Convenience macros for Heron client files
################################################################################
def heron_client_files():
    return heron_client_bin_files() + heron_client_lib_files() + heron_client_conf_files()

def heron_client_bin_files():
    return [
        "//heron/cli/src/python:heron",
    ]

def heron_client_conf_files():
    return [
        "//heron/config/src/yaml:config-internals-yaml",
    ]

def heron_client_lib_files():
    return [
        "//heron/newscheduler/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/uploaders/src/java:heron-localfs-uploader",
        "//heron/statemgrs/src/java:heron-zookeeper-statemgr",
        "//heron/statemgrs/src/java:heron-localfs-statemgr",
        "@protobuf-java//jar",
        "@slf4j-api//jar",
        "@slf4j-jdk//jar", 
        "@log4j-over-slf4j//jar",
    ]

################################################################################
# Convenience macros for Heron core files
################################################################################
def heron_core_files():
    return heron_core_bin_files() + heron_core_conf_files() + heron_core_lib_files()

def heron_core_bin_files():
    return [
        "//heron/controller/src/python:heron-controller",
        "//heron/executor/src/python:heron-executor",
        "//heron/shell/src/python:heron-shell",
        "//heron/stmgr/src/cpp:heron-stmgr",
        "//heron/tmaster/src/cpp:heron-tmaster",
    ]

def heron_core_conf_files():
    return [
        "//heron/instance/src/java:aurora-logging-properties",
        "//heron/config/src/yaml:config-internals-yaml",
        "//heron/config/src/yaml:metrics-sinks-yaml",
    ]

def heron_core_lib_files():
    return [
        "//heron/instance/src/java:heron-instance",
        "//heron/metricsmgr/src/java:heron-metricsmgr",
        "//heron/newscheduler/src/java:heron-scheduler",
        "//heron/statemgrs/src/java:heron-zookeeper-statemgr",
    ]

################################################################################
# Convenience macros for Heron tools files
################################################################################
def heron_tools_files():
    return heron_tools_bin_files() + heron_tools_conf_files() + heron_tools_lib_files()

def heron_tools_bin_files():
    return [
        "//heron/tracker/src/python:heron-tracker",
    ]

def heron_tools_conf_files():
    return []

def heron_tools_lib_files():
    return []

################################################################################
# Macro for running Heron local integration test
################################################################################
def local_heron_test(name, srcs, main, topology, args=None, data=None, deps=None):
    working_dir = name + "-working-directory"
    cli_target = name + "-cli-files"
    core_target = name + "-core-files"
    topology_target = name + "-topology-files"

    install_heron_files(
        name   = cli_target, 
        dir    = working_dir,
        bin    = heron_client_bin_files(), 
        lib    = heron_client_lib_files(), 
        others = heron_client_conf_files(),
    )

    uheron_core_files = [item for item in heron_core_files() if item not in heron_client_files()]
    install_heron_files(
        name   = core_target, 
        dir    = working_dir,
        others = uheron_core_files,
    )

    install_heron_files(
        name   = topology_target,
        dir    = working_dir,
        others = topology
    )
      
    extra_args = [
        "--heron-working-directory " + working_dir,
    ]

    cli_target_data = ":" + cli_target
    core_target_data = ":" + core_target
    topology_target_data = ":" + topology_target

    newargs = args + extra_args if args else extra_args
    native.py_test(
        name = name,
        srcs = srcs,
        main = main,
        data = [
            cli_target_data,
            core_target_data,
            topology_target_data,
        ],
        args = newargs,
        deps = deps,
    )
