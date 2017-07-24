#!/usr/bin/python
# -*- coding: utf-8 -*-

import mmap
import os
import subprocess
import sys
from datetime import datetime, timedelta


def tail(filename, n):
  """Returns last n lines from the filename. No exception handling"""
  size = os.path.getsize(filename)
  with open(filename, "rb") as f:
   fm = mmap.mmap(f.fileno(), 0, mmap.MAP_SHARED, mmap.PROT_READ)
   try:
      for i in xrange(size - 1, -1, -1):
          if fm[i] == '\n':
             n -= 1
             if n == -1:
                break
      return fm[i + 1 if i else 0:].splitlines()
   finally:
        fm.close()

def main(file, cmd):
  print "%s writing to: %s" % (cmd, file)
  with open(file, "w") as out:
   count = 0
   process = subprocess.Popen(cmd,
                              stderr=subprocess.STDOUT,
                              stdout=subprocess.PIPE)
   start = datetime.now()
   nextPrint = start + timedelta(seconds=1)
   # wait for the process to terminate
   pout = process.stdout
   line = pout.readline()
   while line:
       count = count + 1
       if datetime.now() > nextPrint:
          diff = datetime.now() - start
          sys.stdout.write("\r%d seconds %d log lines"%(diff.seconds, count))
          sys.stdout.flush()
          nextPrint = datetime.now() + timedelta(seconds=10)
       out.write(line)
       line = pout.readline()
   out.close()
   errcode = process.wait()
   diff = datetime.now() - start
   sys.stdout.write("\r%d seconds %d log lines"%(diff.seconds, count))
  print "\n %s finished with errcode: %d" % (cmd, errcode)
  if errcode != 0:
     lines = tail(file, 1000)
     print '\n'.join(lines)
     sys.exit(errcode)
  return errcode

if __name__ == "__main__":
  if sys.argv < 1:
      print "Usage: %s [file info]" % sys.argv[0]
      sys.exit(1)
  file = sys.argv[1]
  cmd = sys.argv[2:]
  main(file, cmd)
