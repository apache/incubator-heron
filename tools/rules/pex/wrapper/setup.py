"""Minimal setup.py for pex_wrapper"""

import setuptools

setuptools.setup(
    name="pex_wrapper",
    author="foo",
    author_email="bar",
    url="foo",
    py_modules=["pex_wrapper"],
    version="0.1",
    install_requires=[
        "pex",
        "wheel",
        # Not strictly required, but requests makes SSL more likely to work
        "requests",
    ],
)
