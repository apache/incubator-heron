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
''' config.py '''
import os
import yaml

from heron.statemgrs.src.python.config import Config as StateMgrConfig

STATEMGRS_KEY = "statemgrs"
VIZ_URL_FORMAT_KEY = "viz.url.format"


class Config(object):
  """
  Responsible for reading the yaml config file and
  exposing various tracker configs.
  """

  def __init__(self, conf_file):
    self.configs = None
    self.statemgr_config = StateMgrConfig()
    self.viz_url_format = None

    self.parse_config_file(conf_file)

  def parse_config_file(self, conf_file):
    """parse config files"""
    expanded_conf_file_path = os.path.expanduser(conf_file)
    assert os.path.lexists(expanded_conf_file_path), "Config file does not exists: %s" % (conf_file)

    # Read the configuration file
    with open(expanded_conf_file_path, 'r') as f:
      self.configs = yaml.load(f)

    self.load_configs()

  def load_configs(self):
    """load config files"""
    self.statemgr_config.set_state_locations(self.configs[STATEMGRS_KEY])
    if VIZ_URL_FORMAT_KEY in self.configs:
      self.viz_url_format = self.validated_viz_url_format(self.configs[VIZ_URL_FORMAT_KEY])
    else:
      self.viz_url_format = ""

  # pylint: disable=no-self-use
  def validated_viz_url_format(self, viz_url_format):
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
    dummy_formatted_viz_url = viz_url_format
    for key, value in valid_parameters.iteritems():
      dummy_formatted_viz_url = dummy_formatted_viz_url.replace(key, value)

    # All $ signs must have been replaced
    if '$' in dummy_formatted_viz_url:
      raise Exception("Invalid viz.url.format: %s" % (viz_url_format))

    # No error is thrown, so the format is valid.
    return viz_url_format

  def get_formatted_viz_url(self, execution_state):
    """
    @param execution_state: The python dict representing JSON execution_state
    @return Formatted viz url
    """

    # Create the parameters based on execution state
    valid_parameters = {
        "${CLUSTER}": execution_state["cluster"],
        "${ENVIRON}": execution_state["environ"],
        "${TOPOLOGY}": execution_state["jobname"],
        "${ROLE}": execution_state["role"],
        "${USER}": execution_state["submission_user"],
    }

    formatted_viz_url = self.viz_url_format

    for key, value in valid_parameters.iteritems():
      formatted_viz_url = formatted_viz_url.replace(key, value)

    return formatted_viz_url
