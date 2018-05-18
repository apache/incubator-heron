import os
import unittest2 as unittest

from heron.packer.src.python.store import blobstore
from testfixtures import TempDirectory

class LocalBlobstoreTest(unittest.TestCase):
  def setUp(self):
    self.tmp_dir = TempDirectory()
    self.tmp_dir.makedir("blob")
    self.conf = {
        blobstore.BLOBSTORE_NAME: "LocalBlobstore",
        blobstore.BLOBSTORE_ROOT_PATH: os.path.join(self.tmp_dir.path, "blob"),
        blobstore.BLOBSTORE_SCHEME: "test_scheme",
    }

    self.blobstore = blobstore.LocalBlobstore(self.conf)
    self.not_exists_file = "file/not/exists.txt"
    self.not_valid_dir = "dir/not/valid"
    self.test_file = "test.txt"

    self.tmp_dir.write(self.test_file, b'foo upload test')

  def tearDown(self):
    TempDirectory.cleanup_all()

  def test_upload(self):
    # if file doesn't exist, return False and and empty string
    status, location = self.blobstore.upload(self.not_exists_file)
    self.assertFalse(status)
    self.assertEqual(location, "")

    # if file exists, return True and non empty string indicate the location
    status, location = self.blobstore.upload(os.path.join(self.tmp_dir.path, self.test_file))
    self.assertTrue(status)
    self.assertIsNotNone(location)

  def test_download(self):
    # if downloaded file doesn't exist, return False and no operation
    status = self.blobstore.download(self.not_exists_file, self.tmp_dir.path)
    self.assertFalse(status)

    # if dest path is not valid, return False and no operation
    status = self.blobstore.download(
      os.path.join(self.tmp_dir.path, self.test_file), self.not_valid_dir)
    self.assertFalse(status)

    # if file path and dest are valid, return True and file is downloaded
    src_path = os.path.join(self.tmp_dir.path, self.test_file)
    dest_path = os.path.join(self.tmp_dir.path, "download_test/")
    self.tmp_dir.makedir(dest_path)
    status = self.blobstore.download(src_path, dest_path)
    self.assertTrue(status)
    self.assertTrue(os.path.exists(dest_path))

  def test_delete(self):
    # if file doesn't exist, status is false
    status = self.blobstore.delete(self.not_exists_file)
    self.assertFalse(status)

    # if file exists, verify status is true and the file is deleted
    file_path = os.path.join(self.tmp_dir.path, self.test_file)
    status = self.blobstore.delete(file_path)
    self.assertTrue(status)
    self.assertFalse(os.path.exists(file_path))

  def test_get_scheme(self):
    scheme = self.blobstore.get_scheme()
    self.assertEqual(scheme, self.conf[blobstore.BLOBSTORE_SCHEME])