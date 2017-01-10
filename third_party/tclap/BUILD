licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "tclap"
package_version = "1.2.1"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

include_files = [
    "include/tclap/Arg.h",
    "include/tclap/ArgException.h",
    "include/tclap/ArgTraits.h",
    "include/tclap/CmdLine.h",
    "include/tclap/CmdLineInterface.h",
    "include/tclap/CmdLineOutput.h",
    "include/tclap/Constraint.h",
    "include/tclap/DocBookOutput.h",
    "include/tclap/HelpVisitor.h",
    "include/tclap/IgnoreRestVisitor.h",
    "include/tclap/MultiArg.h",
    "include/tclap/MultiSwitchArg.h",
    "include/tclap/OptionalUnlabeledTracker.h",
    "include/tclap/StandardTraits.h",
    "include/tclap/StdOutput.h",
    "include/tclap/SwitchArg.h",
    "include/tclap/UnlabeledMultiArg.h",
    "include/tclap/UnlabeledValueArg.h",
    "include/tclap/ValueArg.h",
    "include/tclap/ValuesConstraint.h",
    "include/tclap/VersionVisitor.h",
    "include/tclap/Visitor.h",
    "include/tclap/XorHandler.h",
    "include/tclap/ZshCompletionOutput.h",
]

genrule(
    name = "tclap-srcs",
    srcs = [package_file],
    outs = include_files,
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t tclap.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'tar xfz ' + package_file,
        'cd ' + package_dir,
        './configure --prefix=$$INSTALL_DIR --enable-shared=no',
        'make install',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "tclap-cxx",
    srcs = [
        "empty.cc",
    ],
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "tclap",
    srcs = [
        ":tclap-cxx",
    ]
)

filegroup(
    name = "tclap-files",
    srcs = include_files,
)
