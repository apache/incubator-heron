import os
import shutil
import time

from heron.common.src.python.color import Log
from heron.package.src.python.common import utils

class Blobstore(object):
  def upload(self, file_path):
    """Upload the file at file_path into blobstore"""
    """return a stored path"""
    raise NotImplementedError('upload is not implemented')

  def download(self, file_path, dest_path):
    """Download a file in blobstore with file_path to dest_path"""
    raise NotImplementedError('download is not implemented')

  def delete(self, file_path):
    """Delete a file in blobstore with given path"""
    raise NotImplementedError('delete is not implemented')

  def get_scheme(self):
    """Return the storage scheme"""
    raise NotImplementedError('get_scheme is not implemented')

class LocalBlobstore(Blobstore):
  def __init__(self):
    # TODO (nlu): get root path and scheme from config
    self.root_path = "/Users/nlu/blobs"
    self.scheme = "localfs"

  # return a tuple, first field indicates the status of upload
  # second field is the path if succeed
  def upload(self, file_path):
    dest_path = self._resolve_path(file_path)
    if self._copy_file(file_path, dest_path):
      return True, os.path.join(dest_path, os.path.basename(file_path))
    else:
      return False, ""

  def download(self, file_path, dest_file):
    if not self._is_valid_file(file_path):
      Log.error("Requested file does not exist")
      return False

    if not self._is_valid_file(dest_file):
      Log.error("File destination does not exist")
      return False

    if self._copy_file(file_path, dest_file):
      Log.info("%s is successfully downloaded" % os.path.basename(file_path))
      return True
    else:
      return False

  def delete(self, file_path):
    try:
      os.remove(file_path)
      Log.info("%s is successfully deleted" % os.path.basename(file_path))
    except OSError as e:
      Log.error("Failed to delete file: file path is not valid. Exception: %s" % e)
      return False

    return True

  def get_scheme(self):
    return self.scheme

  def _copy_file(self, src, dst):
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

  def _is_valid_file(self, file_path):
    return os.path.exists(file_path)
