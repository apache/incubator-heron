#!/usr/bin/env python

from os.path import abspath, join
import subprocess
import sys
from yaml import dump, load, Loader

from git import Repo

heron_repo_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).strip()
current_heron_version = subprocess.check_output(['git', 'describe', '--tags', '--abbrev=0']).strip()
website_yaml_config_location = 'config.yaml'

with open(website_yaml_config_location, 'r') as config:
    yaml_config = load(config)
    yaml_config_heron_version = yaml_config['params']['versions']['heron']
    if yaml_config_heron_version != current_heron_version:
        yaml_config['params']['versions']['heron'] = current_heron_version

        with open(website_yaml_config_location, 'w') as outfile:
            outfile.write(dump(yaml_config, default_flow_style=False))
        print("Heron version has been updated in config.yaml from %s to %s" % yaml_config_heron_version, current_heron_version)
    else:
        print("Heron version in config.yaml already up to date")
        
sys.exit(0)
