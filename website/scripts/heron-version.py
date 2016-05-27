import os
import subprocess

from git import Repo

heron_repo_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'])
heron_repo = Repo(heron_repo_root)
#print(heron_repo_root.active_branch.name)
print(heron_repo_root)
