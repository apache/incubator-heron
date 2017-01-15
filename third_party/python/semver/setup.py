# -*- coding: utf-8 -*-

from distutils.core import setup

with open('README.md') as f:
    LONG_DESCRIPTION = f.read()

setup(
    name='semver',
    version='2.4.1',
    description='Python package to work with Semantic Versioning (http://semver.org/)',
    long_description=LONG_DESCRIPTION,
    author='Konstantine Rybnikov',
    author_email='k-bx@k-bx.com',
    url='https://github.com/k-bx/python-semver',
    download_url='https://github.com/k-bx/python-semver/downloads',
    py_modules=['semver'],
    include_package_data=True,
    license='BSD',
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
