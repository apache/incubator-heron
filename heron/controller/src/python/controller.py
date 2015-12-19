# A simple webclient that contacts a given port on the localhost
# and does activate/deactivate

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
