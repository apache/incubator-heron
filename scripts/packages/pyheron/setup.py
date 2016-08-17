"""Pyheron setuptools based setup module"""
import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
requirement_file = os.path.join(here, 'requirements.txt')
with open(requirement_file, 'r') as f:
  raw_requirements = f.read().strip()

requirements = raw_requirements.split('\n')
print "Requirements: %s" % requirements

long_description = "Pyheron package allows a developer to write python topology in Heron. " \
                   "Pyheron is backward compatible with the popular python API called " \
                   "streamparse. It can be run in various clusters including Mesos/Aurora, " \
                   "Mesos/Marathon, YARN, etc."

setup(
  name='pyheron',
  version='0.0.1',
  description='Python Topology API for Heron',
  long_description=long_description,

  url='https://github.com/twitter/heron',

  author='Heron Release Team',
  author_email='heronstreaming@gmail.com',

  classifiers=[
    'Development Status :: 3 - Alpha',

    'Intended Audience :: Developers',
    'Topic :: Real Time streaming',

    'Programming Language :: Python :: 2.7',
  ],

  keywords='heron topology python',
  packages=find_packages(),

  install_requires=requirements
)
