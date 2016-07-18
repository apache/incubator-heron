"""A blobstore module defines the interfaces for storing blob object"""
import os
import shutil
import sys
import time

from heron.common.src.python.color import Log
from heron.package.src.python.common import utils

BLOBSTORE_NAME = "heron.package.blobstore.name"
BLOBSTORE_ROOT_PATH = "heron.package.blobstore.root_path"
BLOBSTORE_SCHEME = "heron.package.blobstore.scheme"

def get_blobstore(conf):
  """get blobstore instance with retrospection"""
  blobstore = getattr(sys.modules[__name__], conf[BLOBSTORE_NAME])
  instance = blobstore(conf)
  return instance

class Blobstore(object):
  """Interfaces for blobstore"""
  def get_scheme(self):
    """return the storage scheme"""
    raise NotImplementedError('get_scheme is not implemented')

  def upload(self, file_path):
    """upload the file at file_path into blobstore and return a stored location"""
    raise NotImplementedError('upload is not implemented')

  def download(self, file_path, dest_path):
    """download a file in blobstore with file_path to dest_path"""
    raise NotImplementedError('download is not implemented')

  def delete(self, file_path):
    """delete a file in blobstore with given path"""
    raise NotImplementedError('delete is not implemented')

class LocalBlobstore(Blobstore):
  """Local file system based implementation of Blobstore"""
  def __init__(self, conf):
    self.root_path = conf[BLOBSTORE_ROOT_PATH]
    self.scheme = conf[BLOBSTORE_SCHEME]

  def get_scheme(self):
    return self.scheme

  def upload(self, file_path):
    if not self._is_valid_file(file_path):
      Log.error("Upload file doesn't exist")
      return False, ""

    dest_path = self._resolve_path(file_path)
    if self._copy_file(file_path, dest_path):
      return True, os.path.join(dest_path, os.path.basename(file_path))
    else:
      return False, ""

  def download(self, file_path, dest_path):
    if not self._is_valid_file(file_path):
      Log.error("Requested file does not exist")
      return False

    if not self._is_valid_file(dest_path):
      Log.error("File destination '%s' does not exist" % dest_path)
      return False

    if self._copy_file(file_path, dest_path):
      Log.info("%s is successfully downloaded" % os.path.basename(file_path))
      return True
    else:
      return False

  def delete(self, file_path):
    if not self._is_valid_file(file_path):
      Log.error("Requested file does not exist")
      return False

    try:
      os.remove(file_path)
      Log.info("%s is successfully deleted" % os.path.basename(file_path))
    except OSError as e:
      Log.error("Failed to delete file: file path is not valid. Exception: %s" % e)
      return False

    return True

  @staticmethod
  def _copy_file(src, dst):
    try:
      if not os.path.exists(dst):
        os.makedirs(dst)

      shutil.copy(src, dst)
    except IOError as e:
      Log.error("Unable to copy file %s to location %s. Exception: %s" % (src, dst, e))
      return False

    return True

  def _resolve_path(self, file_path):
    md5 = utils.get_md5(file_path)
    # use md5 and timestamp for a file's dir to avoid collision
    dest_path = os.path.join(self.root_path, md5[0:4], md5[4:], str(int(time.time())))
    return dest_path

  @staticmethod
  def _is_valid_file(file_path):
    return os.path.exists(file_path)
