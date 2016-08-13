"""Pyheron setuptools based setup module"""
import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
requirements = ['colorlog==2.6.1',
                'protobuf==2.5.0',
                'setuptools==18.0.1']

setup(
  name='pyheron',
  version='0.0.1',
  description='Python API for Heron topology',
  long_description='Long description about pyheron',

  url='https://github.com/twitter/heron',

  author='Heron Release Team',
  author_email='streaming-compute@twitter.com',

  classifiers=[
    'Development Status :: 3 - Alpha',

    'Intended Audience :: Developers',
    'Topic :: Realtime streaming',

    'Programming Language :: Python :: 2.7',
  ],

  keywords='heron topology python',
  packages=find_packages(),

  install_requires=requirements
)