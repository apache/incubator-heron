import os
import unittest2 as unittest

from heron.packer.src.python.common import constants
from heron.packer.src.python.store import metastore
from mock import patch
from testfixtures import TempDirectory

class LocalMetastoreTest(unittest.TestCase):
  def setUp(self):
    self.tmp_dir = TempDirectory()
    self.meta_path = os.path.join(self.tmp_dir.path, "meta.json")
    self.conf = {
      metastore.METASTORE_NAME: "LocalMetastore",
      metastore.METASTORE_ROOT_PATH: self.meta_path,
    }
    self.metastore = metastore.LocalMetastore(self.conf)
    self.extra_info = "extra/information/stub"

  def tearDown(self):
    TempDirectory.cleanup_all()

  @patch('heron.packer.src.python.store.metastore.LocalMetastore._load_meta_file')
  def test_get_packages(self, mock_load_meta):
    # mock the meta file
    list_of_packages = ["p1", "p2", "p3", "p4"]
    test_role = "test_role"
    pkg_dict = dict((pkg, "Test") for pkg in list_of_packages)
    mock_load_meta.return_value = {test_role: pkg_dict}
    packages = self.metastore.get_packages(test_role, self.extra_info)
    self.assertItemsEqual(packages, list_of_packages)

  @patch('heron.packer.src.python.store.metastore.LocalMetastore._load_meta_file')
  def test_get_versions(self, mock_load_meta):
    # mock the meta file
    test_role = "test_role"
    test_pkg = "test_pkg"
    version_dict = {
      constants.LIVE: 2,
      constants.LATEST: 2,
      constants.COUNTER: 3,
      "1": {constants.DELETED: True, constants.DESC: "version 1"},
      "2": {constants.DELETED: False, constants.DESC: "version 2"}
    }
    mock_load_meta.return_value = {test_role:{test_pkg: version_dict}}
    expected_versions = ['Version 1 DELETED\n\tversion 1', 'Version 2\n\tversion 2']

    version_info = self.metastore.get_versions(test_role, test_pkg, self.extra_info)
    self.assertItemsEqual(version_info, expected_versions)

  @patch('heron.packer.src.python.store.metastore.LocalMetastore._dump_meta_file')
  @patch('heron.packer.src.python.store.metastore.LocalMetastore._load_meta_file')
  def test_add_pkg_meta(self, mock_load_meta, mock_dump_meta):
    mock_load_meta.return_value = {}
    mock_dump_meta.side_effect = None

    role = "role"
    pkg_name = "pkg_name"
    description = "test package"
    location = self.meta_path

    status, version = self.metastore.add_pkg_meta(
        role, pkg_name, location, description, self.extra_info)
    self.assertTrue(status)
    self.assertEqual("1", version)

  @patch('heron.packer.src.python.store.metastore.LocalMetastore._load_meta_file')
  def test_get_pkg_meta(self, mock_load_meta):
    role = "test_role"
    no_role = "no_role"
    pkg_name = "test_pkg"
    no_pkg_name = "no_test_pkg"
    version = "1"
    no_version = "100"

    expect_meta = dict((v, str(v)) for v in range(1,3))

    version_dict = {
      constants.LATEST: "1",
      constants.LIVE: "1",
      constants.COUNTER: 1,
      "1": expect_meta
    }

    mock_load_meta.return_value = {role: {pkg_name: version_dict}}

    # invalid role provided
    meta = self.metastore.get_pkg_meta(no_role, pkg_name, version, self.extra_info)
    self.assertIsNone(meta)

    # invalid package name provided
    meta = self.metastore.get_pkg_meta(role, no_pkg_name, version, self.extra_info)
    self.assertIsNone(meta)

    # invalid version provided
    meta = self.metastore.get_pkg_meta(role, pkg_name, no_version, self.extra_info)
    self.assertIsNone(meta)

    # correct information provided
    meta = self.metastore.get_pkg_meta(role, pkg_name, version, self.extra_info)
    self.assertItemsEqual(meta, expect_meta)

    # LATEST tag provided
    meta = self.metastore.get_pkg_meta(role, pkg_name, constants.LATEST, self.extra_info)
    self.assertItemsEqual(meta, expect_meta)

    # LIVE tag provided
    meta = self.metastore.get_pkg_meta(role, pkg_name, constants.LIVE, self.extra_info)
    self.assertItemsEqual(meta, expect_meta)

  @patch('heron.packer.src.python.store.metastore.LocalMetastore._load_meta_file')
  def test_get_pkg_location(self, mock_load_meta):
    role = "role"
    no_role = "no_role"
    pkg_name = "pkg_name"
    no_pkg_name = "no_pkg_name"
    version = "1"
    no_version = "100"

    store_path = "test/store/path"
    expect_meta = {constants.STORE_PATH: store_path}
    version_dict = {
      constants.LATEST: "1",
      constants.LIVE: "1",
      constants.COUNTER: 1,
      "1": expect_meta
    }

    mock_load_meta.return_value = {role: {pkg_name: version_dict}}

    # invalid role provided
    path = self.metastore.get_pkg_location(no_role, pkg_name, version, self.extra_info)
    self.assertIsNone(path)

    # invalid package name provided
    path = self.metastore.get_pkg_location(role, no_pkg_name, version, self.extra_info)
    self.assertIsNone(path)

    # invalid version provided
    path = self.metastore.get_pkg_location(role, pkg_name, no_version, self.extra_info)
    self.assertIsNone(path)

    # correct information provided
    path = self.metastore.get_pkg_location(role, pkg_name, version, self.extra_info)
    self.assertItemsEqual(path, store_path)

    # LATEST tag provided
    path = self.metastore.get_pkg_location(role, pkg_name, constants.LATEST, self.extra_info)
    self.assertItemsEqual(path, store_path)

    # LIVE tag provided
    path = self.metastore.get_pkg_location(role, pkg_name, constants.LIVE, self.extra_info)
    self.assertItemsEqual(path, store_path)

  def test_get_pkg_uri(self):
    scheme = "test_scheme"
    role = "test_role"
    pkg_name = "test_pkg"
    version = "version"
    expected_uri = "%s://%s/%s/%s" % (scheme, role, pkg_name, version)
    uri = self.metastore.get_pkg_uri(scheme, role, pkg_name, version, self.extra_info)
    self.assertEqual(expected_uri, uri)

  @patch('heron.packer.src.python.store.metastore.LocalMetastore._load_meta_file')
  def test_delete_pkg_meta(self, mock_load_meta):
    role = "role"
    no_role = "no_role"
    pkg_name = "pkg_name"
    no_pkg_name = "no_pkg_name"
    deleted_version = "1"
    live_version = "2"
    version = "3"
    no_version = "100"

    version_dict = {
      constants.LIVE: live_version,
      constants.LATEST: version,
      constants.COUNTER: 3,
      deleted_version: {constants.DELETED: True, constants.DESC: "version 1"},
      live_version: {constants.DELETED: False, constants.DESC: "version 2"},
      version: {constants.DELETED: False, constants.DESC: "version 3"},
    }
    mock_load_meta.return_value = {role:{pkg_name: version_dict}}

    # invalid role provided
    status = self.metastore.delete_pkg_meta(no_role, pkg_name, version, self.extra_info)
    self.assertFalse(status)

    # invalid package name provided
    status = self.metastore.delete_pkg_meta(role, no_pkg_name, version, self.extra_info)
    self.assertFalse(status)

    # invalid version provided
    status = self.metastore.delete_pkg_meta(role, pkg_name, no_version, self.extra_info)
    self.assertFalse(status)

    # live version provided
    status = self.metastore.delete_pkg_meta(role, pkg_name, live_version, self.extra_info)
    self.assertFalse(status)

    # deleted version provided
    status = self.metastore.delete_pkg_meta(no_role, pkg_name, deleted_version, self.extra_info)
    self.assertFalse(status)

    # correct version provided
    status = self.metastore.delete_pkg_meta(role, pkg_name, version, self.extra_info)
    self.assertTrue(status)

  @patch('heron.packer.src.python.store.metastore.LocalMetastore._load_meta_file')
  def test_set_tag(self, mock_load_meta):
    role = "role"
    no_role = "no_role"
    pkg_name = "pkg_name"
    no_pkg_name = "no_pkg_name"
    version = "1"
    no_version = "100"

    version_dict = {
      constants.LATEST: "1",
      constants.LIVE: "1",
      constants.COUNTER: 1,
      "1": {"a": "info"},
    }

    mock_load_meta.return_value = {role: {pkg_name: version_dict}}

    # invalid role provided
    status = self.metastore.set_tag(constants.LIVE, no_role, pkg_name, version, self.extra_info)
    self.assertFalse(status)

    # invalid package name provided
    status = self.metastore.set_tag(constants.LIVE, role, no_pkg_name, version, self.extra_info)
    self.assertFalse(status)

    # invalid version provided
    status = self.metastore.set_tag(constants.LIVE, role, pkg_name, no_version, self.extra_info)
    self.assertFalse(status)

    # correct information provided
    status = self.metastore.set_tag(constants.LIVE, role, pkg_name, version, self.extra_info)
    self.assertTrue(status)

  @patch('heron.packer.src.python.store.metastore.LocalMetastore._load_meta_file')
  def test_unset_tag(self, mock_load_meta):
    role = "role"
    no_role = "no_role"
    pkg_name = "pkg_name"
    no_pkg_name = "no_pkg_name"
    no_version = "100"

    version_dict = {
      constants.LATEST: "1",
      constants.LIVE: "1",
      constants.COUNTER: 1,
      "1": {"a": "info"},
    }

    mock_load_meta.return_value = {role: {pkg_name: version_dict}}

    status = self.metastore.unset_tag(constants.LIVE, role, pkg_name, self.extra_info)
    self.assertTrue(status)