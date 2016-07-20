"""A metastore module defines the interface for storing meta information"""
import json
import os
import sys

from heron.common.src.python.color import Log
from heron.package.src.python.common import constants

METASTORE_NAME = "heron.package.metastore.name"
METASTORE_ROOT_PATH = "heron.package.metastore.root_path"

def get_metastore(conf):
  """Get metastore instance with retrospection"""
  metastore = getattr(sys.modules[__name__], conf[METASTORE_NAME])
  instance = metastore(conf)
  return instance

class Metastore(object):
  """Metastore Interface"""

  def get_packages(self, role, extra_info):
    """Get all package names"""
    raise NotImplementedError('get_packages is not implemented')

  def get_versions(self, role, pkg_name, extra_info):
    """Get all available version numbers for a package"""
    raise NotImplementedError('get_versions is not implemented')

  def add_pkg_meta(self, role, pkg_name, location, description, extra_info):
    """Add meta data for a package's version"""
    raise NotImplementedError('add_pkg_meta in not implemented')

  def delete_pkg_meta(self, role, pkg_name, version, extra_info):
    """Delete meta data for a package's version"""
    raise NotImplementedError('delete_pkg_meta is not implemented')

  def get_pkg_meta(self, role, pkg_name, version, extra_info):
    """Get meta data for a package's version"""
    raise NotImplementedError('get_pkg_meta is not implemented')

  def get_pkg_location(self, role, pkg_name, version, extra_info):
    """Get package stored location"""
    raise NotImplementedError('get_pkg_location is not implemented')

  def set_tag(self, tag, role, pkg_name, version, extra_info):
    """Set the tag on a package, only "LIVE" and "LATEST" tags are available"""
    raise NotImplementedError('set_tag is not implemented')

  def unset_tag(self, tag, role, pkg_name, extra_info):
    """Clean the tag on a package"""
    raise NotImplementedError('unset_tag is not implemented')

  def get_meta_by_tag(self, tag, role, pkg_name, extra_info):
    """Get a pkg's info by tag, only "LIVE" and "LATEST" tags are available"""
    raise NotImplementedError('get_meat_by_tag is not implemented')

  def get_pkg_uri(self, scheme, role, pkg_name, version, extra_info):
    """Get pkg uri"""
    raise NotImplementedError('get_pkg_uri is not implemented')

class LocalMetastore(Metastore):
  """Local file system based implementation of Metastore

     The LocalMetastore stores meta information as a json file and has the following format:
     {"role1":
         {"package1":
             {"live": None,
              "latest": 1,
              "counter": 3,
              "1": {"deleted": True, "store_location": "foo/bar/p1.jar", "description": "foo bar"},
              "2": {"deleted": False, ...},
               ...
            }
             ....
         }
       ....
     }
  """
  def __init__(self, conf):
    # get meta file path from config
    self.meta_file = conf[METASTORE_ROOT_PATH]

    # create the meta file if missing
    if not os.path.exists(self.meta_file):
      with open(self.meta_file, "w") as meta:
        meta.write("{}")

  def get_packages(self, role, extra_info):
    packages = []

    meta_info = self._load_meta_file()

    if role in meta_info:
      # append all encountered package names
      pkg_map = meta_info[role]
      for key, _ in pkg_map.items():
        packages.append(key)
    else:
      Log.error("role %s not found" % role)

    return packages

  def get_versions(self, role, pkg_name, extra_info):
    versions = []

    meta_info = self._load_meta_file()

    if role in meta_info and pkg_name in meta_info[role]:
      version_map = meta_info[role][pkg_name]
      for key, value in version_map.items():
        # ignore tag entries in the version_map
        if key != constants.LIVE and \
           key != constants.LATEST and \
           key != constants.COUNTER:
          version_msg = LocalMetastore._build_version_message(key, value)
          versions.append(version_msg)
    else:
      Log.error("role/pkg (%s/%s) not found" % (role, pkg_name))

    # dict entry is unordered, sorting for readability
    versions.sort()

    return versions

  def add_pkg_meta(self, role, pkg_name, location, description, extra_info):
    meta_info = self._load_meta_file()

    # add role into meta_info if doesn't exist
    if role not in meta_info:
      meta_info[role] = {}

    # add pkg_name into meta_info[role] if doesn't exist
    if pkg_name not in meta_info[role]:
      pkg_entry = {}
      # live version
      pkg_entry[constants.LIVE] = None
      # largest valid version, 0 means no valid version
      pkg_entry[constants.LATEST] = 0
      # next available version
      pkg_entry[constants.COUNTER] = 0
      # add pkg_entry into meta_info
      meta_info[role][pkg_name] = pkg_entry

    # get the next available version number
    meta_info[role][pkg_name][constants.COUNTER] += 1
    # version will be used as a json map key, so it's converted to string
    version = str(meta_info[role][pkg_name][constants.COUNTER])

    # create the version info for a new version
    version_entry = {}
    version_entry[constants.STORE_PATH] = location
    version_entry[constants.DELETED] = False
    version_entry[constants.DESC] = description

    # add new version entry
    meta_info[role][pkg_name][version] = version_entry

    # update latest version number
    meta_info[role][pkg_name][constants.LATEST] = version

    # update meta file
    self._dump_meta_file(meta_info)

    return True, version

  def get_pkg_meta(self, role, pkg_name, version, extra_info):
    meta_info = self._load_meta_file()

    if not self._is_valid_pkg(meta_info, role, pkg_name):
      Log.error("requested role/pkg (%s/%s) not found" % (role, pkg_name))
      return False

    # version must be less than LATEST version or be LIVE/LATEST tag
    pkg_info = meta_info[role][pkg_name]
    if not self._is_valid_version(pkg_info, version):
      Log.error("requested version %s is not correct" % version)
      return None

    # translate to actual version if "LIVE" and "LATEST" tag provided
    version = self._translate_version(version, pkg_info)

    return pkg_info[version]

  def get_pkg_location(self, role, pkg_name, version, extra_info):
    version_info = self.get_pkg_meta(role, pkg_name, version, extra_info)
    return version_info[constants.STORE_PATH]

  def get_pkg_uri(self, scheme, role, pkg_name, version, extra_info):
    return "%s://%s/%s/%s" % (scheme, role, pkg_name, version)

  def delete_pkg_meta(self, role, pkg_name, version, extra_info):
    meta_info = self._load_meta_file()

    if not self._is_valid_pkg(meta_info, role, pkg_name):
      Log.error("requested role/pkg (%s/%s) not found" % (role, pkg_name))
      return False

    pkg_info = meta_info[role][pkg_name]
    if not self._is_valid_version(pkg_info, version):
      Log.error("requested version %s is not correct" % version)
      return False

    # delete live is not allowed
    if version == constants.LIVE or pkg_info[constants.LIVE] == version:
      Log.error("Cannot delete a 'live' version")
      return False

    # delete latest requires update of the latest tag
    if version == constants.LATEST or pkg_info[constants.LATEST] == version:
      version = pkg_info[constants.LATEST]
      # find the second latest version
      pkg_info[constants.LATEST] = self._find_second_latest(pkg_info)

    # check if the version is already deleted
    if pkg_info[version][constants.DELETED]:
      Log.error("version already deleted")
      return False

    # mark version deletion and update meta file
    meta_info[role][pkg_name][version][constants.DELETED] = True
    self._dump_meta_file(meta_info)

    return True

  @staticmethod
  def _find_second_latest(pkg_info):
    # return 0 if no valid second latest is found
    if pkg_info[constants.LATEST] is None:
      return "0"

    latest = int(pkg_info[constants.LATEST])
    for v in range(latest - 1, 0, -1):
      version = str(v)
      if not pkg_info[version][constants.DELETED]:
        return version

    return "0"

  def get_meta_by_tag(self, tag, role, pkg_name, extra_info):
    meta_info = self._load_meta_file()

    if not self._is_valid_pkg(meta_info, role, pkg_name):
      Log.error("requested role/pkg (%s/%s) not found" % (role, pkg_name))
      return dict()

    pkg_info = meta_info[role][pkg_name]
    version = pkg_info[tag]
    if (tag == constants.LIVE and version is None) or \
            (tag == constants.LATEST and version == "0"):
      Log.error("The tag '%s' is unset. No package version found" % tag)
      return dict()
    else:
      info = pkg_info[version]
      info[constants.VERSION] = version
      return info

  def set_tag(self, tag, role, pkg_name, version, extra_info):
    return self._update_tag(tag, role, pkg_name, version)

  def unset_tag(self, tag, role, pkg_name, extra_info):
    return self._update_tag(tag, role, pkg_name, None, True)

  def _update_tag(self, tag, role, pkg_name, version=None, is_reset=False):
    meta_info = self._load_meta_file()

    if not self._is_valid_pkg(meta_info, role, pkg_name):
      Log.error("requested role/pkg (%s/%s) not found" % (role, pkg_name))
      return False

    pkg_info = meta_info[role][pkg_name]

    # reset tag to initial value; set to provided version otherwise
    if is_reset:
      if tag == constants.LATEST:
        meta_info[role][pkg_name][tag] = "0"
      elif tag == constants.LIVE:
        meta_info[role][pkg_name][tag] = None
    else:
      if not self._is_valid_version(pkg_info, version):
        Log.error("requested version %s is not correct" % version)
        return False

      meta_info[role][pkg_name][tag] = version

    self._dump_meta_file(meta_info)
    return True

  @staticmethod
  def _is_valid_pkg(meta_info, role, pkg_name):
    valid_pkg = False
    if pkg_name in meta_info.get(role, {}):
      valid_pkg = True

    return valid_pkg

  @staticmethod
  def _is_valid_version(pkg_info, version):
    # a valid version must be either a tag name or a version # less than LATEST
    return version == constants.LATEST or \
           version == constants.LIVE or \
           pkg_info[constants.LATEST] >= version

  @staticmethod
  def _translate_version(version, pkg_info):
    # if a version is a tag name, convert it to the actual version
    if version == constants.LATEST:
      if pkg_info[constants.LATEST] == 0:
        Log.error("there's no version available for the pkg. Bailing out...")
      version = str(pkg_info[constants.LATEST])

    elif version == constants.LIVE:
      if pkg_info[constants.LIVE] is None:
        Log.error("there's no live version for the pkg. Bailing out...")
        sys.exit(1)
      version = str(pkg_info[constants.LIVE])

    return version

  def _load_meta_file(self):
    with open(self.meta_file, "r") as meta:
      meta_info = json.load(meta)

    return meta_info

  def _dump_meta_file(self, meta_info):
    with open(self.meta_file, "w") as meta_file:
      json.dump(meta_info, meta_file)

  @staticmethod
  def _build_version_message(version, info):
    # current version message is in the following format:
    # Version 1 DELETED
    #     "DESCRIPTION INFORMATION"
    message = "Version %s" % version
    if info[constants.DELETED]:
      message += " DELETED"
    message += "\n\t%s" % info[constants.DESC]

    return message