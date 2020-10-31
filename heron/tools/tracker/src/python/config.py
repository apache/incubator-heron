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
    self.extra_links = []

    self.load_configs()

  def load_configs(self):
    """load config files"""
    self.statemgr_config.set_state_locations(self.configs[STATEMGRS_KEY])
    if EXTRA_LINKS_KEY in self.configs:
      for extra_link in self.configs[EXTRA_LINKS_KEY]:
        self.extra_links.append(self.validate_extra_link(extra_link))

  def validate_extra_link(self, extra_link: dict) -> None:
    """validate extra link"""
    if EXTRA_LINK_NAME_KEY not in extra_link or EXTRA_LINK_FORMATTER_KEY not in extra_link:
      raise Exception("Invalid extra.links format. " +
                      "Extra link must include a 'name' and 'formatter' field")

    self.validated_formatter(extra_link[EXTRA_LINK_FORMATTER_KEY])

  def validated_formatter(self, url_format: str) -> None:
    """Check visualization url format has no unrecongnised parameters."""
    # collect the parameters which would be interpolated
    formatter_variables = set()
    class ValidationHelper:
      def __getitem__(self, key):
        formatter_variables.add(key)
        return ""

    string.Template(url_format).safe_substitute(ValidationHelper())

    if not formatter_variables <= self.FORMATTER_PARAMETERS:
      raise Exception(f"Invalid viz.url.format: {url_format!r}")

  @staticmethod
  def get_formatted_url(formatter: str, execution_state: dict) -> str:
    """
    Format a url string using values from the execution state.

    """

    subs = {
        var: execution_state[prop]
        for prop, var in (
            ("cluster", "CLUSTER"),
            ("environ", "ENVIRON"),
            ("jobname", "TOPOLOGY"),
            ("role", "ROLE"),
            ("submission_user", "USER"))
        if prop in execution_state
    }
    return string.Template(formatter).substitute(subs)

  def __str__(self):
    return "".join((self.config_str(c) for c in self.configs[STATEMGRS_KEY]))

  @staticmethod
  def config_str(config):
    keys = ("type", "name", "hostport", "rootpath", "tunnelhost")
    return "".join("\t{k}: {config[k]}\n" for k in keys if k in config).rstrip()
