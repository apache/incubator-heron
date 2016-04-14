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

import os
import pkgutil
import yaml

class Config:
  """
  Responsible for reading the yaml config files and
  exposing state locations through python methods.
  A state location is represented by following dictionary:
    {
      type: zookeeper (or file),
      name: unique_name_of_the_location,
      hostport: if_hosted_on_remote_host:port,
      rootpath: /root/path/for/states
    }
  """

  def __init__(self, conf_file):

    # Read the configuration file
    with open(conf_file, 'r') as f:
      self.locations = yaml.load(f)

    self.validate_state_locations()

  def validate_state_locations(self):
    """
    Names of all state locations must be unique.
    """
    names = map(lambda loc: loc["name"], self.locations)
    assert len(names) == len(set(names)), "Names of state locations must be unique"

  def get_all_state_locations(self):
    """
    Returns all the state locations.
    """
    return self.locations

  def get_state_locations_of_type(self, location_type):
    """
    Return a list of state locations of a particular type.
    """
    return filter(lambda location: location['type'] == location_type, self.locations)

