"""Pyheron setuptools based setup module"""
import os, sys
from setuptools import setup, find_packages

# read the requirements file
here = os.path.abspath(os.path.dirname(__file__))

# get the version from release yaml file
release_yaml = os.path.join(here, 'release.yaml')
with open(release_yaml, 'r') as f:
  file_lines = f.readlines()
  for line in file_lines:
    split_line = line.strip().split(" : ")
    if 'version' in split_line[0]:
      version =  split_line[1].strip("'")
release_version = version.replace("/", ".")


# read the requirements file
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
  version=release_version,
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
