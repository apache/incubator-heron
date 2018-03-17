licenses(["notice"])
  
package(default_visibility = ["//visibility:public"])

pkg_name = "javacc"
pkg_version = "5.0"

package_file = pkg_name + "-" + pkg_version + ".tar.gz"
package_dir = pkg_name + "-" + pkg_version

out_files = [
    "bin/lib/javacc.jar",
    "bin/jjtree.bat",
    "bin/jjrun",
    "bin/javacc",
    "bin/javacc.bat",
    "bin/jjdoc.bat",
    "bin/jjtree",
    "bin/jjdoc",
]

genrule(
    name = "javacc-srcs",
    srcs = [
        package_file, 
    ],
    outs = out_files,
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t libevent.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'tar xfz ' + package_file,
        'cd ' + package_dir,
        'cp -r . $$INSTALL_DIR',
        'rm -rf $$TMP_DIR',
    ]),
)

filegroup(
    name = "javacc",
    srcs = [
        "bin/javacc",
    ],
)
