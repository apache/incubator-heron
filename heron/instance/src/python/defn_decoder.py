import sys
from heron.proto import topology_pb2

if __name__ == '__main__':
    if len(sys.argv) != 2:
      print ("Usage: " + sys.argv[0] + " <defn file>" )
      sys.exit(1)
    topology = topology_pb2.Topology()
    with open(sys.argv[1], "rb") as f:
      topology.ParseFromString(f.read())

    print topology

