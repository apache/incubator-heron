# Copyright 2017 Twitter. All rights reserved.
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
'''dispatcher.py'''
import heron.tools.cli.src.python.fetcher as fetcher

all_fetchers = [fetcher.LocalFetcher]

def extract(package):
  ''' extract protocol and package location '''
  sep = "://"
  if sep not in package:
    return "file", package
  else:
    chunks = package.split(sep)
    if len(chunks) != 2 or '' in chunks:
      raise Exception(
          "Failed to get protocol of the package location: %s" % package)
    else:
      return chunks[0], chunks[1]

# pylint: disable=dangerous-default-value
def fetch(package, fetchers=all_fetchers):
  protocol, loc = extract(package)
  for f in fetchers:
    if f.protocol == protocol:
      return f.fetch(loc)
  raise Exception("No available fetcher for protocol %s" % protocol)
