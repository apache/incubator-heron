#!/usr/bin/env python
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

from heron.statemgrs.src.python.config import Config as StateMgrConfig

STATEMGRS_KEY = "statemgrs"
EXTRA_LINKS_KEY = "extra.links"
EXTRA_LINK_NAME_KEY = "name"
EXTRA_LINK_FORMATTER_KEY = "formatter"
EXTRA_LINK_URL_KEY = "url"

class Config(object):
  """
  Responsible for reading the yaml config file and
  exposing various tracker configs.
  """

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

  def validate_extra_link(self, extra_link):
    """validate extra link"""
    if EXTRA_LINK_NAME_KEY not in extra_link or EXTRA_LINK_FORMATTER_KEY not in extra_link:
      raise Exception("Invalid extra.links format. " +
                      "Extra link must include a 'name' and 'formatter' field")

    self.validated_formatter(extra_link[EXTRA_LINK_FORMATTER_KEY])
    return extra_link

  # pylint: disable=no-self-use
  def validated_formatter(self, url_format):
    """validate visualization url format"""
    # We try to create a string by substituting all known
    # parameters. If an unknown parameter is present, an error
    # will be thrown
    valid_parameters = {
        "${CLUSTER}": "cluster",
        "${ENVIRON}": "environ",
        "${TOPOLOGY}": "topology",
        "${ROLE}": "role",
        "${USER}": "user",
    }
    dummy_formatted_url = url_format
    for key, value in list(valid_parameters.items()):
      dummy_formatted_url = dummy_formatted_url.replace(key, value)

    # All $ signs must have been replaced
    if '$' in dummy_formatted_url:
      raise Exception("Invalid viz.url.format: %s" % (url_format))

    # No error is thrown, so the format is valid.
    return url_format

  def get_formatted_url(self, formatter, execution_state):
    """
    @param formatter: The template string to interpolate
    @param execution_state: The python dict representing JSON execution_state
    @return Formatted viz url
    """

    # Create the parameters based on execution state
    common_parameters = {
        "${CLUSTER}": execution_state.get("cluster", "${CLUSTER}"),
        "${ENVIRON}": execution_state.get("environ", "${ENVIRON}"),
        "${TOPOLOGY}": execution_state.get("jobname", "${TOPOLOGY}"),
        "${ROLE}": execution_state.get("role", "${ROLE}"),
        "${USER}": execution_state.get("submission_user", "${USER}"),
    }

    formatted_url = formatter

    for key, value in list(common_parameters.items()):
      formatted_url = formatted_url.replace(key, value)

    return formatted_url

  def __str__(self):
    return "".join((self.config_str(c) for c in self.configs[STATEMGRS_KEY]))

  def config_str(self, config):
    keys = ("type", "name", "hostport", "rootpath", "tunnelhost")
    return "".join("\t{}: {}\n".format(k, config[k]) for k in keys if k in config).rstrip()
