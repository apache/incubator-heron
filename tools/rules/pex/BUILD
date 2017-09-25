exports_files(["pex_rules.bzl", "testlauncher.sh.template"])

# Can't use pex_binary to build this one, so we have this elaborate ritual:
genrule(
    name = "pex_wrapper",
    srcs = [
        "wrapper/setup.py",
        "wrapper/pex_wrapper.py",
        "wrapper/README",
        "@setuptools_src//file",
        "@wheel_src//file",
        "@pex_src//file",
        "@requests_src//file",
    ],
    tools = ["@virtualenv//:virtualenv"],
    outs = ["pex_wrapper.pex"],
    executable = True,
    output_to_bindir = True,
    message = "Bootstrapping pex",
    cmd = """
        OUTDIR=$$(cd $(@D) && pwd)
        # Workaround really long shebang lines breaking on linux:
        # Use a /tmp path, but keep the actual venv inside the bazel outdir.
        # Avoids having to worry about cleanup, even if sandboxing is off.
        TMPF=$$(mktemp -d -t pex.XXXXX)
        ln -sf "$$OUTDIR" "$$TMPF"
        VENV="$${TMPF}/venv"

        $(location @virtualenv//:virtualenv) --no-download --quiet --clear "$$VENV"
        PYTHON="$$VENV/bin/python"

        $$VENV/bin/pip install pex \
            --quiet --no-cache-dir --no-index --build $(@D)/pexbuild \
            --find-links $$(dirname $(location @pex_src//file)) \
            --find-links $$(dirname $(location @wheel_src//file)) \
            --find-links $$(dirname $(location @setuptools_src//file))

        # Work around setuptools insistance on writing to the source directory,
        # which is discouraged by Bazel (and annoying)
        cp -r $$(dirname $(location wrapper/setup.py)) $(@D)/.pex_wrapper

        # Use the bootstrapped pex to build pex_wrapper.pex
        $$VENV/bin/pex $(@D)/.pex_wrapper \
            --disable-cache --no-index -m pex_wrapper -o $@ \
            --find-links $$(dirname $(location @pex_src//file)) \
            --find-links $$(dirname $(location @setuptools_src//file)) \
            --find-links $$(dirname $(location @requests_src//file)) \
            --find-links $$(dirname $(location @wheel_src//file))
    """,
    visibility = ["//visibility:public"],
)
