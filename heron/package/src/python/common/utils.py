import hashlib
import os
import yaml

BLOCK_SIZE = 4096
CONF = "conf"
PACKER_YAML = "packer.yaml"

def get_md5(file_path):
  m = hashlib.md5()
  with open(file_path, "rb") as file:
    while True:
      block = file.read(BLOCK_SIZE)
      if not block:
        break
      m.update(block)
  return m.hexdigest()

def print_list(list, raw):
  if raw:
    print list
  else:
    print "\n".join(list)

def print_dict(dict, raw):
  if raw:
    print dict
  else:
    for key, value in dict.items():
      print "%s: %s" % (key, value)

def get_packer_conf_path():
  heron_dir = os.path.join("/".join(os.path.realpath( __file__ ).split('/')[:-7]))
  conf_path = os.path.join(heron_dir, CONF, PACKER_YAML)
  return conf_path

def load_packer_conf():
  conf_path = get_packer_conf_path()

  with open(conf_path, "r") as conf_file:
    conf = yaml.load(conf_file)

  return conf