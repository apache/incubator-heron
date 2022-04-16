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

''' config.py '''
import string

from heron.statemgrs.src.python.config import Config as StateMgrConfig

STATEMGRS_KEY = "statemgrs"
EXTRA_LINKS_KEY = "extra.links"
EXTRA_LINK_NAME_KEY = "name"
EXTRA_LINK_FORMATTER_KEY = "formatter"
EXTRA_LINK_URL_KEY = "url"

class Config:
  """
  Responsible for reading the yaml config file and
  exposing various tracker configs.
  """
  FORMATTER_PARAMETERS = {"CLUSTER", "ENVIRON", "TOPOLOGY", "ROLE", "USER"}

  def __init__(self, configs):
    self.configs = configs
    self.statemgr_config = StateMgrConfig()
    self.statemgr_config.set_state_locations(configs[STATEMGRS_KEY])

    self.extra_links = configs.get(EXTRA_LINKS_KEY, [])
    for link in self.extra_links:
      self.validate_extra_link(link)

  @classmethod
  def validate_extra_link(cls, extra_link: dict) -> None:
    """validate extra link"""
    if EXTRA_LINK_NAME_KEY not in extra_link or EXTRA_LINK_FORMATTER_KEY not in extra_link:
      raise Exception("Invalid extra.links format. " +
                      "Extra link must include a 'name' and 'formatter' field")

    cls.validated_formatter(extra_link[EXTRA_LINK_FORMATTER_KEY])

  @classmethod
  def validated_formatter(cls, url_format: str) -> None:
    """Check visualization url format has no unrecongnised parameters."""
    # collect the parameters which would be interpolated
    formatter_variables = set()
    class ValidationHelper:
      def __getitem__(self, key):
        formatter_variables.add(key)
        return ""

    string.Template(url_format).safe_substitute(ValidationHelper())

    if not formatter_variables <= cls.FORMATTER_PARAMETERS:
      raise Exception(f"Invalid viz.url.format: {url_format!r}")

  def __str__(self):
    return "".join(self.config_str(c) for c in self.configs[STATEMGRS_KEY])

  @staticmethod
  def config_str(config):
    keys = ("type", "name", "hostport", "rootpath", "tunnelhost")
    # pylint: disable=consider-using-f-string
    return "".join("\t{}: {}\n".format(k, config[k]) for k in keys if k in config).rstrip()
