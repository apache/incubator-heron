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

'''
utils.py
Contains utility functions used by tracker.
'''

import json
import os
import sys
import subprocess

from asyncio import iscoroutinefunction
from functools import wraps
from pathlib import Path
from typing import Any, Generic, Literal, Optional, TypeVar

from heron.common.src.python.utils.log import Log
from heron.tools.tracker.src.python import constants
from heron.proto import topology_pb2

import javaobj.v1 as javaobj
import yaml

from fastapi import APIRouter, HTTPException
from pydantic import Field
from pydantic.generics import GenericModel


# directories for heron tools distribution
BIN_DIR = "bin"
CONF_DIR = "conf"
LIB_DIR = "lib"

ResultType = TypeVar("ResultType")


class ResponseEnvelope(GenericModel, Generic[ResultType]):
  execution_time: float = Field(0.0, alias="executiontime")
  message: str
  result: Optional[ResultType] = None
  status: Literal[
      constants.RESPONSE_STATUS_FAILURE, constants.RESPONSE_STATUS_SUCCESS
  ]
  tracker_version: str = constants.API_VERSION

class BadRequest(HTTPException):
  """Raised when bad input is recieved."""
  def __init__(self, detail: str = None) -> None:
    super().__init__(400, detail)

class EnvelopingAPIRouter(APIRouter):
  """Router which wraps response_models with ResponseEnvelope."""

  def api_route(self, response_model=None, **kwargs):
    """This provides the decorator used by router.<method>."""
    if not response_model:
      return super().api_route(response_model=response_model, **kwargs)

    wrapped_response_model = ResponseEnvelope[response_model]
    decorator = super().api_route(response_model=wrapped_response_model, **kwargs)

    @wraps(decorator)
    def new_decorator(f):
      if iscoroutinefunction(f):
        @wraps(f)
        async def envelope(*args, **kwargs):
          result = await f(*args, **kwargs)
          return wrapped_response_model(
              result=result,
              execution_time=0.0,
              message="ok",
              status="success",
          )
      else:
        @wraps(f)
        def envelope(*args, **kwargs):
          result = f(*args, **kwargs)
          return wrapped_response_model(
              result=result,
              execution_time=0.0,
              message="ok",
              status="success",
          )
      return decorator(envelope)

    return new_decorator


def make_shell_endpoint(topology_info: dict, instance_id: int) -> str:
  """
  Makes the http endpoint for the heron shell
  if shell port is present, otherwise returns None.

  """
  # Format: container_<id>_<instance_id>
  pplan = topology_info["physical_plan"]
  stmgrId = pplan["instances"][instance_id]["stmgrId"]
  host = pplan["stmgrs"][stmgrId]["host"]
  shell_port = pplan["stmgrs"][stmgrId]["shell_port"]
  return f"http://{host}:{shell_port}"

def make_shell_job_url(host: str, shell_port: int, _) -> Optional[str]:
  """
  Make the job url from the info
  stored in stmgr. This points to dir from where
  all the processes are started.
  If shell port is not present, it returns None.

  """
  if not shell_port:
    return None
  return f"http://{host}:{shell_port}/browse/"

def make_shell_logfiles_url(
    host: str,
    shell_port: int,
    _: Any,
    instance_id: int = None,
) -> Optional[str]:
  """
  Make the url for log-files in heron-shell
  from the info stored in stmgr.
  If no instance_id is provided, the link will
  be to the dir for the whole container.
  If shell port is not present, it returns None.

  """
  if not shell_port:
    return None
  if not instance_id:
    return f"http://{host}:{shell_port}/browse/log-files"
  return f"http://{host}:{shell_port}/file/log-files/{instance_id}.log.0"

def make_shell_logfile_data_url(
    host: str,
    shell_port: int,
    instance_id: int,
    offset: int,
    length: int,
) -> str:
  """
  Make the url for log-file data in heron-shell
  from the info stored in stmgr.
  """
  return (
      f"http://{host}:{shell_port}"
      f"/filedata/log-files/{instance_id}.log.0"
      f"?offset={offset}&length={length}"
  )

def make_shell_filestats_url(host: str, shell_port: int, path: str) -> str:
  """
  Make the url for filestats data in heron-shell
  from the info stored in stmgr.

  """
  return f"http://{host}:{shell_port}/filestats/{path}"

################################################################################
# Get normalized class path depending on platform
################################################################################
def cygpath(x: str) -> str:
  """
  This will return the path of input arg for windows
  :return: the path in windows
  """
  command = ['cygpath', '-wp', x]
  p = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
  output, _ = p.communicate()
  lines = output.split("\n")
  return lines[0]

def normalized_class_path(x: str) -> str:
  """
  This will return the class path depending on the platform
  :return: the class path
  """
  if sys.platform == 'cygwin':
    return cygpath(x)
  return x

################################################################################
# Get the root of heron tracker dir and various sub directories
################################################################################
def get_heron_tracker_dir() -> str:
  """
  This will extract heron tracker directory from .pex file.
  :return: root location for heron-tools.
  """
  # assuming the tracker runs from $HERON_ROOT/bin/heron-tracker
  root = Path(sys.argv[0]).resolve(strict=True).parent.parent
  return normalized_class_path(str(root))

def get_heron_tracker_bin_dir() -> str:
  """
  This will provide heron tracker bin directory from .pex file.
  :return: absolute path of heron lib directory
  """
  bin_path = os.path.join(get_heron_tracker_dir(), BIN_DIR)
  return bin_path

def get_heron_tracker_conf_dir() -> str:
  """
  This will provide heron tracker conf directory from .pex file.
  :return: absolute path of heron conf directory
  """
  conf_path = os.path.join(get_heron_tracker_dir(), CONF_DIR)
  return conf_path

def parse_config_file(config_file: str) -> Optional[str]:
  """
  This will parse the config file for the tracker
  :return: the config or None if the file is not found
  """
  expanded_config_file_path = os.path.expanduser(config_file)
  if not os.path.lexists(expanded_config_file_path):
    return None

  # Read the configuration file
  with open(expanded_config_file_path, 'r') as f:
    return yaml.load(f)

################################################################################
# utils for parsing protobuf key-value pairs from the API
################################################################################
def convert_pb_kvs(kvs, include_non_primitives=True) -> dict:
  """
  converts pb kvs to dict
  """
  config = {}
  for kv in kvs:
    if kv.value:
      config[kv.key] = kv.value
    elif kv.serialized_value:
      # add serialized_value support for python values (fixme)

      # is this a serialized java object
      if topology_pb2.JAVA_SERIALIZED_VALUE == kv.type:
        jv = _convert_java_value(kv, include_non_primitives=include_non_primitives)
        if jv is not None:
          config[kv.key] = jv
      else:
        config[kv.key] = _raw_value(kv)
  return config

def _convert_java_value(kv, include_non_primitives=True):
  try:
    pobj = javaobj.loads(kv.serialized_value)
    if isinstance(pobj, str):
      return pobj

    if isinstance(pobj, javaobj.transformers.DefaultObjectTransformer.JavaPrimitiveClass):
      return pobj.value

    if include_non_primitives:
      # java objects that are not strings return value and encoded value
      # Hexadecimal byte array for Serialized objects that
      return {
          'value' : json.dumps(pobj,
                               default=lambda custom_field: custom_field.__dict__,
                               sort_keys=True,
                               indent=2),
          'raw' : kv.serialized_value.hex()}

    return None
  except Exception:
    Log.exception("Failed to parse data as java object")
    if include_non_primitives:
      return _raw_value(kv)
    return None

def _raw_value(kv):
  return {
      # The value should be a valid json object
      'value' : '{}',
      'raw' : kv.serialized_value.hex()}
