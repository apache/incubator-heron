#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

'''pex_loader.py: module for dynamically loading pex'''

import os
import re
import sys
import zipimport
import zipfile

from heron.common.src.python.utils.log import Log

egg_regex = r"^(\.deps\/[^\/\s]*\.(egg|whl))\/"

def _get_deps_list(abs_path_to_pex):
  """Get a list of paths to included dependencies in the specified pex file

  Note that dependencies are located under `.deps` directory
  """
  with zipfile.ZipFile(abs_path_to_pex, mode='r') as pex:
    deps = list({re.match(egg_regex, i).group(1) for i in pex.namelist()
                if re.match(egg_regex, i) is not None})
  return deps

def load_pex(path_to_pex, include_deps=True):
  """Loads pex file and its dependencies to the current python path"""
  abs_path_to_pex = os.path.abspath(path_to_pex)
  Log.debug(f"Add a pex to the path: {abs_path_to_pex}")
  if abs_path_to_pex not in sys.path:
    sys.path.insert(0, os.path.dirname(abs_path_to_pex))

  # add dependencies to path
  if include_deps:
    for dep in _get_deps_list(abs_path_to_pex):
      to_join = os.path.join(os.path.dirname(abs_path_to_pex), dep)
      if to_join not in sys.path:
        Log.debug(f"Add a new dependency to the path: {dep}")
        sys.path.insert(0, to_join)

  Log.debug(f"Python path: {str(sys.path)}")

def resolve_heron_suffix_issue(abs_pex_path, class_path):
  """Resolves duplicate package suffix problems

  When dynamically loading a pex file and a corresponding python class (bolt/spout/topology),
  if the top level package in which to-be-loaded classes reside is named 'heron', the path conflicts
  with this Heron Instance pex package (heron.instance.src.python...), making the Python
  interpreter unable to find the target class in a given pex file.
  This function resolves this issue by individually loading packages with suffix `heron` to
  avoid this issue.

  However, if a dependent module/class that is not directly specified under ``class_path``
  and has conflicts with other native heron packages, there is a possibility that
  such a class/module might not be imported correctly. For example, if a given ``class_path`` was
  ``heron.common.src.module.Class``, but it has a dependent module (such as by import statement),
  ``heron.common.src.python.dep_module.DepClass`` for example, pex_loader does not guarantee that
  ``DepClass` is imported correctly. This is because ``heron.common.src.python.dep_module`` is not
  explicitly added to sys.path, while ``heron.common.src.python`` module exists as the native heron
  package, from which ``dep_module`` cannot be found, so Python interpreter may raise ImportError.

  The best way to avoid this issue is NOT to dynamically load a pex file whose top level package
  name is ``heron``. Note that this method is included because some of the example topologies and
  tests have to have a pex with its top level package name of ``heron``.
  """
  # import top-level package named `heron` of a given pex file
  # pylint: disable=no-member
  importer = zipimport.zipimporter(abs_pex_path)
  importer.load_module("heron")

  # remove 'heron' and the classname
  to_load_lst = class_path.split('.')[1:-1]
  loaded = ['heron']
  loaded_mod = None
  for to_load in to_load_lst:
    # pylint: disable=no-member
    sub_importer = zipimport.zipimporter(os.path.join(abs_pex_path, '/'.join(loaded)))
    loaded_mod = sub_importer.load_module(to_load)
    loaded.append(to_load)

  return loaded_mod


def import_and_get_class(path_to_pex, python_class_name):
  """Imports and load a class from a given pex file path and python class name

  For example, if you want to get a class called `Sample` in
  /some-path/sample.pex/heron/examples/src/python/sample.py,
  ``path_to_pex`` needs to be ``/some-path/sample.pex``, and
  ``python_class_name`` needs to be ``heron.examples.src.python.sample.Sample``
  """
  abs_path_to_pex = os.path.abspath(path_to_pex)

  Log.debug(f"Add a pex to the path: {abs_path_to_pex}")
  Log.debug(f"In import_and_get_class with cls_name: {python_class_name}")
  split = python_class_name.split('.')
  from_path = '.'.join(split[:-1])
  import_name = python_class_name.split('.')[-1]

  Log.debug(f"From path: {from_path}, import name: {import_name}")

  # Resolve duplicate package suffix problem (heron.), if the top level package name is heron
  if python_class_name.startswith("heron."):
    try:
      mod = resolve_heron_suffix_issue(abs_path_to_pex, python_class_name)
      return getattr(mod, import_name)
    except:
      Log.error(f"Could not resolve class {python_class_name} with special handling")

  mod = __import__(from_path, fromlist=[import_name], level=0)
  Log.debug(f"Imported module: {str(mod)}")
  return getattr(mod, import_name)
