import sys

from heron.package.src.python.common import package
from heron.package.src.python.common import parser
from heron.package.src.python.common import utils
from heron.package.src.python.store import blobstore
from heron.package.src.python.store import metastore

def get_packer(conf):
  blobstore_object = blobstore.get_blobstore(conf)
  metastore_object = metastore.get_metastore(conf)

  packer = package.HeronPackage(metastore_object, blobstore_object)
  return packer

def main():
  cmd_parser = parser.create_parser()

  # if no argument is provided, print help and exit
  if len(sys.argv[1:]) == 0:
    cmd_parser.print_help()
    sys.exit(1)

  namespace = vars(cmd_parser.parse_args())
  command = namespace['command']
  if command == "help":
    parser.help(cmd_parser, namespace)
  else:
    conf = utils.load_packer_conf()
    packer = get_packer(conf)
    packer_commands = parser.get_commands(packer)
    packer_commands.get(command)(namespace)

if __name__ == "__main__":
  main()
