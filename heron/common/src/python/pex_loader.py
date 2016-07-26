# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''pex_loader.py: module for dynamically loading pex'''

import os
import re
import sys
import zipimport
import zipfile

from heron.common.src.python.log import Log

# TODO: Add whl regex as well
egg_regex = r"^(\.deps\/[^\/\s]*\.egg)\/"

def _get_deps_list(abs_path_to_pex):
  """Get a list of paths to included dependencies in the specified pex file

  Note that dependencies are located under `.deps` directory
  """
  pex = zipfile.ZipFile(abs_path_to_pex, mode='r')
  deps = list(set([re.match(egg_regex, i).group(1) for i in pex.namelist()
                   if re.match(egg_regex, i) is not None]))
  return deps

def load_pex(path_to_pex, include_deps=True):
  """Loads pex file and its dependencies to the current python path"""
  abs_path_to_pex = os.path.abspath(path_to_pex)
  Log.debug("Add a pex to the path: " + abs_path_to_pex)
  sys.path.insert(0, abs_path_to_pex)

  # add dependencies to path
  if include_deps:
    for dep in _get_deps_list(abs_path_to_pex):
      Log.debug("Add a new dependency to the path: " + dep)
      sys.path.insert(0, os.path.join(abs_path_to_pex, dep))

  Log.debug("Python path: " + str(sys.path))

def resolve_heron_suffix_issue(abs_pex_path):
  """Resolves duplicate package suffix problems

  When dynamically loading a pex file and a corresponding python class (bolt/spout/topology),
  if the top level package in which to-be-loaded classes reside is named 'heron', the path conflicts
  with this Heron Instance pex package (heron.instance.src.python...), making the Python
  interpreter unable to find the target class in a given pex file.
  This function resolves this issue by individually loading packages with suffix `heron` to
  avoid this issue.

  **Note that this function assumes that there is no user-defined code/package
  under `heron.instance`, `heron.proto` and `heron.common` packages, so they wouldn't be imported**
  """
  # import top-level package named `heron` of a given pex file
  importer = zipimport.zipimporter(abs_pex_path)
  importer.load_module("heron")

def import_and_get_class(path_to_pex, python_class_name):
  """Imports and load a class from a given pex file path and python class name

  For example, if you want to get a class called `Sample` in
  /some-path/sample.pex/heron/examples/src/python/sample.py,
  ``path_to_pex`` needs to be ``/some-path/sample.pex``, and
  ``python_class_name`` needs to be ``heron.examples.src.python.sample.Sample``
  """
  abs_path_to_pex = os.path.abspath(path_to_pex)

  Log.debug("Add a pex to the path: " + abs_path_to_pex)
  Log.debug("In import_and_get_class with cls_name: " + python_class_name)
  split = python_class_name.split('.')
  from_path = '.'.join(split[:-1])
  import_name = python_class_name.split('.')[-1]

  Log.debug("From path: " + from_path + " -- " + "Import name: " + import_name)

  # Resolve duplicate package suffix problem (heron.), if the top level package name is heron
  if python_class_name.startswith("heron."):
    resolve_heron_suffix_issue(abs_path_to_pex)

  mod = __import__(from_path, fromlist=[import_name], level=-1)
  Log.debug("Imported module: " + str(mod))
  return getattr(mod, import_name)
