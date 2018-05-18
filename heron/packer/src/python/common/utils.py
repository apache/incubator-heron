"""package utils module"""
import hashlib
import os
import yaml

BLOCK_SIZE = 4096
CONF = "conf"
PACKER_YAML = "packer.yaml"

def get_md5(file_path):
  """get the md5 checksum for a give 'file'"""
  m = hashlib.md5()
  with open(file_path, "rb") as input_file:
    while True:
      block = input_file.read(BLOCK_SIZE)
      if not block:
        break
      m.update(block)
  return m.hexdigest()

def print_list(a_list, raw):
  """print a raw list if 'raw' is True; line by line if 'raw' is False"""
  if raw:
    print a_list
  else:
    print "\n".join(a_list)

def print_dict(a_dict, raw):
  """print a raw dict if 'raw' is True; line by line if 'raw' is False"""
  if raw:
    print a_dict
  else:
    for key, value in a_dict.items():
      print "%s: %s" % (key, value)

def get_packer_conf_path():
  """get the config file path"""
  heron_dir = os.path.join("/".join(os.path.realpath(__file__).split('/')[:-7]))
  conf_path = os.path.join(heron_dir, CONF, PACKER_YAML)
  return conf_path

def load_packer_conf():
  """load yaml config file"""
  conf_path = get_packer_conf_path()

  with open(conf_path, "r") as conf_file:
    conf = yaml.load(conf_file)

  return conf
