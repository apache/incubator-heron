"""A blobstore module defines the interface for storing blob object"""
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
  """Get blobstore instance with retrospection"""
  blobstore = getattr(sys.modules[__name__], conf[BLOBSTORE_NAME])
  instance = blobstore(conf)
  return instance

class Blobstore(object):
  """Blobstore interface"""

  def get_scheme(self):
    """Return the storage scheme"""
    raise NotImplementedError('get_scheme is not implemented')

  def upload(self, file_path):
    """Upload the file at file_path to blobstore and return (status, stored_location)"""
    raise NotImplementedError('upload is not implemented')

  def download(self, file_path, dest_path):
    """Download a file from blobstore with file_path to dest_path"""
    raise NotImplementedError('download is not implemented')

  def delete(self, file_path):
    """Delete a file from blobstore with  file_path"""
    raise NotImplementedError('delete is not implemented')

class LocalBlobstore(Blobstore):
  """Local file system based implementation of Blobstore"""
  def __init__(self, conf):
    # get root path for storing blob and scheme from config
    self.root_path = conf[BLOBSTORE_ROOT_PATH]
    self.scheme = conf[BLOBSTORE_SCHEME]

  def get_scheme(self):
    return self.scheme

  def upload(self, file_path):
    # verify the file to be uploaded exists
    if not LocalBlobstore._is_valid_file(file_path):
      Log.error("Upload file does not exist")
      return False, ""

    # get the path to store the file
    dest_path = LocalBlobstore._resolve_path(self.root_path, file_path)

    # copy file to destination
    if LocalBlobstore._copy_file(file_path, dest_path):
      return True, os.path.join(dest_path, os.path.basename(file_path))
    else:
      return False, ""

  def download(self, file_path, dest_path):
    if not LocalBlobstore._is_valid_file(file_path):
      Log.error("Requested file does not exist")
      return False

    if not LocalBlobstore._is_valid_file(dest_path):
      Log.error("File destination '%s' is not valid" % dest_path)
      return False

    if LocalBlobstore._copy_file(file_path, dest_path):
      Log.info("%s is successfully downloaded" % os.path.basename(file_path))
      return True
    else:
      return False

  def delete(self, file_path):
    if not LocalBlobstore._is_valid_file(file_path):
      Log.error("Requested file does not exist")
      return False

    try:
      # remove the file from local file system
      os.remove(file_path)
      Log.info("%s is successfully deleted" % os.path.basename(file_path))
    except OSError as e:
      Log.error("Failed to delete file: file path is not valid. Exception: %s" % e)
      return False

    return True

  @staticmethod
  def _copy_file(src, dst):
    try:
      # create directory if the destination directory doesn't exist
      if not os.path.exists(dst):
        os.makedirs(dst)

      shutil.copy(src, dst)
    except IOError as e:
      Log.error("Unable to copy file %s to location %s. Exception: %s" % (src, dst, e))
      return False

    return True

  @staticmethod
  def _resolve_path(root_path, file_path):
    # path format is: root_path/md5[0:4]/md5[4:]/timestamp/file_name
    # md5 and timestamp are used to avoid collision
    md5 = utils.get_md5(file_path)
    dest_path = os.path.join(root_path, md5[0:4], md5[4:], str(int(time.time())))
    return dest_path

  @staticmethod
  def _is_valid_file(file_path):
    # validate the file path exists and is a file
    return os.path.exists(file_path) and os.path.isfile(file_path)
