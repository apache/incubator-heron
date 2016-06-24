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

import argparse

################################################################################
# Retrieve the given subparser from parser
################################################################################
def get_subparser(parser, command):
  subparsers_actions = [
    action for action in parser._actions
    if isinstance(action, argparse._SubParsersAction)
  ]

  # there will probably only be one subparser_action,
  # but better save than sorry
  for subparsers_action in subparsers_actions:
    # get all subparsers
    for choice, subparser in subparsers_action.choices.items():
      if choice == command:
        return subparser
  return None

