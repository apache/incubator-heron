import hashlib

BLOCK_SIZE = 4096

def get_md5(file_path):
  m = hashlib.md5()
  with open(file_path, "rb") as file:
    while True:
      block = file.read(BLOCK_SIZE)
      if not block:
        break
      m.update(block)
  return m.hexdigest()

def print_list(list, raw):
  if raw:
    print list
  else:
    print "\n".join(list)

def print_dict(dict, raw):
  if raw:
    print dict
  else:
    for key, value in dict.items():
      print "%s: %s" % (key, value)
