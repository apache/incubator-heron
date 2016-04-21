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

# A simple webclient that contacts a given port on the localhost
# and does activate/deactivate

#!/usr/bin/env python2.7

import httplib
import sys
import urllib

def make_request(host, port, url, topologyid):
  try:
    conn = httplib.HTTPConnection(host, port)
    params = {"topologyid" : topologyid}
    fullurl = url + "?" + urllib.urlencode(params)
    conn.request("GET", fullurl)
    response = conn.getresponse()
    if response.status != httplib.OK:
      print fullurl + " failed with " + str(response.status) + " code"
      sys.exit(1)
    else:
      print fullurl + " succeeded"
      sys.exit(0)
  except Exception as e:
    print "Failed to make http request to tmaster"
    print type(e)
    print e.args
    print e
    sys.exit(1)

def main(argv):
  host = argv[1]
  port = int(argv[2])
  url = argv[3]
  topologyid = argv[4]
  make_request(host, port, url, topologyid)

if __name__ == '__main__':
  main(sys.argv)
