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

