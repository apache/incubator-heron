#!/usr/bin/env python

from os.path import abspath, join
import subprocess
from yaml import dump, load, Loader

from git import Repo

heron_repo_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'])
heron_version = subprocess.check_output(['git', 'describe', '--tags', '--abbrev=0'])
website_yaml_config_location = 'config.yaml'

with open(website_yaml_config_location, 'r') as config:
    yaml_config = load(config)
    yaml_config['params']['versions']['heron'] = heron_version.strip()

    with open(website_yaml_config_location, 'w') as outfile:
        outfile.write(dump(yaml_config, default_flow_style=False))
    print("Version has been updated in config.yaml to %s" % heron_version)
