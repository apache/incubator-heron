import sys

from heron.common.src.python.color import Log
from heron.package.src.python.common import constants
from heron.package.src.python.common import utils

class Package(object):
  def add_version(self, args):
    raise NotImplementedError("add_version is not implemented")

  def download(self, args):
   raise NotImplementedError("download is not implemented")

  def delete_version(self, args):
    raise NotImplementedError("delete_version is not implemented")

  def list_packages(self, args):
    raise NotImplementedError("list_packages is not implemented")

  def list_versions(self, args):
    raise NotImplementedError("list_versions is not implemented")

  def set_live(self, args):
    raise NotImplementedError("set_live is not implemented")

  def unset_live(self, args):
    raise NotImplementedError("unset_live is not implemented")

  def show_version(self, args):
    raise NotImplementedError("show_version is not implemented")

  def show_live(self, args):
    raise NotImplementedError("show_live is not implemented")

  def show_latest(self, args):
    raise NotImplementedError("show_latest is not implemented")

  def clean(self, args):
    raise NotImplementedError("clean is not implemented")

class HeronPackage(Package):
  def __init__(self, metastore, blobstore):
    self.metastore = metastore
    self.blobstore = blobstore

  def add_version(self, args):
    """Upload a given package file with role, package_name, description and extra args"""
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    description = args[constants.DESC]
    extra_info = args[constants.EXTRA]
    file_path = args[constants.PKG_PATH]

    # 1. upload file
    successful, store_location = self.blobstore.upload(file_path)
    if not successful:
      Log.error("Failed to upload the package. Bailing out...")
      sys.exit(1)

    # 2. add package version info into metastore
    successful, version = self.metastore.add_pkg_meta(
      role, pkg_name, store_location, description, extra_info)
    if not successful:
      Log.error("Failed to add the meta data. Bailing out...")
      sys.exit(1)

    # 3. return a uri referencing this uploaded package in customized format
    pkg_uri = self.metastore.get_pkg_uri(
      self.blobstore.get_scheme(), role, pkg_name, version, extra_info)
    Log.info("Uploaded package uri: %s" % pkg_uri)
    return pkg_uri

  def download(self, args):
    """Download the package file referenced by pkg_uri to current dir"""
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    version = args[constants.VERSION]
    extra_info = args[constants.EXTRA]
    dest_path = args[constants.DEST_PATH]

    # 1. find package location
    pkg_location = self.metastore.get_pkg_location(role, pkg_name, version, extra_info)
    if pkg_location is None:
      Log.error("Cannot find requested package. Bailing out...")
      sys.exit(1)

    # 2. download
    if self.blobstore.download(pkg_location, dest_path):
      Log.info("Successfully downloaded package")
    else:
      Log.error("Failed to download pacakge")

  def delete_version(self, args):
    """Delete the package file referenced by pkg_uri"""
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    version = args[constants.VERSION]
    extra_info = args[constants.EXTRA]

    # 1. find package location
    pkg_location = self.metastore.get_pkg_location(role, pkg_name, version, extra_info)
    if pkg_location is None:
      Log.error("Cannot find requested package. Bailing out...")
      sys.exit(1)

    # 2. modify the metastore info & write back
    #  live cannot be deleted;
    #  latest needs to be updated if it's deleted
    self.metastore.delete_pkg_meta(role, pkg_name, version, extra_info)

    # 3. try to delete the package(deletion operation failure can be handled in clean cmd)
    isSuccess = self.blobstore.delete(pkg_location)
    if not isSuccess:
      Log.error("meta info updated but blob file still exists. Can be cleaned with the clean cmd.")

  def list_packages(self, args):
    role = args[constants.ROLE]
    extra_info = args[constants.EXTRA]
    is_raw = args[constants.RAW]

    packages = self.metastore.get_packages(role, extra_info)
    utils.print_list(packages, is_raw)

  def list_versions(self, args):
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    extra_info = args[constants.EXTRA]
    is_raw = args[constants.RAW]

    versions = self.metastore.get_versions(role, pkg_name, extra_info)
    utils.print_list(versions, is_raw)

  def set_live(self, args):
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    version = args[constants.VERSION]
    extra_info = args[constants.EXTRA]

    if self.metastore.set_tag(constants.LIVE, role, pkg_name, version, extra_info):
      Log.info("Set live success")
    else:
      Log.error("Set live failed")

  def unset_live(self, args):
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    extra_info = args[constants.EXTRA]

    if self.metastore.unset_tag(constants.LIVE, role, pkg_name, extra_info):
      Log.info("Unset live success")
    else:
      Log.error("Unset live failed")

  def show_version(self, args):
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    version = args[constants.VERSION]
    extra_info = args[constants.EXTRA]
    is_raw = args[constants.RAW]

    pkg_info = self.metastore.get_pkg_meta(role, pkg_name, version, extra_info)
    utils.print_dict(pkg_info, is_raw)

  def show_live(self, args):
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    extra_info = args[constants.EXTRA]
    is_raw = args[constants.RAW]

    live_pkg_info = self.metastore.get_meta_by_tag(constants.LIVE, role, pkg_name, extra_info)
    utils.print_dict(live_pkg_info, is_raw)

  def show_latest(self, args):
    role = args[constants.ROLE]
    pkg_name = args[constants.PKG]
    extra_info = args[constants.EXTRA]
    is_raw = args[constants.RAW]

    latest_pkg_info = self.metastore.get_meta_by_tag(constants.LATEST, role, pkg_name, extra_info)
    utils.print_dict(latest_pkg_info, is_raw)

  # clean the inconsistency state of package store.
  def clean(self):
    pass