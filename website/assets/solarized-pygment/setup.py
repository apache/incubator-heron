#!/usr/bin/python

from setuptools import setup, find_packages

setup(
    name='pygments-solarized',
    version='0.2',
    description='Pygments version of the solarized theme.',
    keywords='pygments style solarized',
    license='BSD',
    author='John Louis Del Rosario, Hank Gay, John Mastro, Brandon Bennett',
    author_email='john2x@gmail.com',

    url='https://github.com/john2x/solarized-pygment',

    packages = find_packages(),
    install_requires=['pygments >= 1.4'],

    entry_points='''
    [pygments.styles]
    solarized=pygments_solarized:SolarizedStyle
    solarized_dark=pygments_solarized:SolarizedDarkStyle
    solarized_dark256=pygments_solarized:SolarizedDark256Style
    ''',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
