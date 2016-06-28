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

# This is a temporary main file to execute stmgr_client.py

from heron.proto import physical_plan_pb2

from network.stmgr_client import StmgrClient
import network.mock_protobuf as mock


def print_usage():
  print "Usage: ./main.py [port]"

def main():
  #if len(sys.argv) != 2:
  #  print_usage()
  #  sys.exit(1)

  sample_instance = mock.get_mock_instance()

  client = StmgrClient("localhost", 8080, "topology--name", "topology--id",
                       sample_instance, None, None, None)
  try:
    # try to establish a connection with localhost:1234
    client.start()
  except KeyboardInterrupt:
    print "Keyboard Interrupt -- bye"
  finally:
    client.stop()

if __name__ == '__main__':
  main()
