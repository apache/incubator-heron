exports_files([
    "pex_rules.bzl",
    "testlauncher.sh.template",
])

# Can't use pex_binary to build this one, so we have this elaborate ritual:
PRE_EXECUTE = [
    "OUTDIR=$$(cd $(@D) && pwd)",
]

DARWIN_EXECUTE = [
    "# Workaround really long shebang lines breaking on linux:",
    "# Use a /tmp path, but keep the actual venv inside the bazel outdir.",
    "# Avoids having to worry about cleanup, even if sandboxing is off.",
    "TMPF=$$(mktemp -d -t pex.XXXXX)",
]

LINUX_EXECUTE = [
    "# Workaround really long shebang lines breaking on linux:",
    "# Use a /tmp path, but keep the actual venv inside the bazel outdir.",
    "# Avoids having to worry about cleanup, even if sandboxing is off.",
    "TMPF=$$(mktemp -d -p /tmp pex.XXXXX)",
]

POST_EXECUTE = [
    'ln -sf "$$OUTDIR" "$$TMPF"',
    'VENV="$${TMPF}/venv"',
    # Create the virtual environment
    'python3 -m venv $$VENV --clear',
    'VIRTUAL_ENV_DISABLE_PROMPT=1 source "$$VENV/bin/activate"',
    'TEMP="$(@D)/pexbuild"',
    'pip install pex \
            --quiet --no-cache-dir --no-index \
            --find-links $$(dirname $(location @pex_pkg//file)) \
            --find-links $$(dirname $(location @wheel_pkg//file)) \
            --find-links $$(dirname $(location @setuptools_pkg//file))',

    '# Work around setuptools insistance on writing to the source directory,',
    '# which is discouraged by Bazel (and annoying)',
    'cp -r $$(dirname $(location wrapper/setup.py)) $(@D)/.pex_wrapper',

    '# Use the bootstrapped pex to build pex_wrapper.pex',
    'pex $(@D)/.pex_wrapper \
            --disable-cache --no-index \
            --entry-point=pex_wrapper \
            --output-file=$@ \
            --find-links $$(dirname $(location @pex_pkg//file)) \
            --find-links $$(dirname $(location @setuptools_pkg//file)) \
            --find-links $$(dirname $(location @requests_pkg//file)) \
            --find-links $$(dirname $(location @charset_pkg//file)) \
            --find-links $$(dirname $(location @idna_pkg//file)) \
            --find-links $$(dirname $(location @urllib3_pkg//file)) \
            --find-links $$(dirname $(location @certifi_pkg//file)) \
            --find-links $$(dirname $(location @wheel_pkg//file))',
   ]

genrule(
    name = "pex_wrapper",
    srcs = [
        "wrapper/setup.py",
        "wrapper/pex_wrapper.py",
        "wrapper/README",
        "@setuptools_pkg//file",
        "@wheel_pkg//file",
        "@pex_pkg//file",
        "@requests_pkg//file",
        "@charset_pkg//file",
        "@idna_pkg//file",
        "@urllib3_pkg//file",
        "@certifi_pkg//file",
    ],
    outs = ["pex_wrapper.pex"],
    cmd = select({
        "@platforms//os:osx": "\n".join(PRE_EXECUTE + DARWIN_EXECUTE + POST_EXECUTE),
        "//conditions:default": "\n".join(PRE_EXECUTE + LINUX_EXECUTE + POST_EXECUTE),
    }),
    executable = True,
    message = "Bootstrapping pex",
    output_to_bindir = True,
    visibility = ["//visibility:public"],
)
