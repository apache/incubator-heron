import json
import os
import sys

from heron.common.src.python.color import Log
from heron.package.src.python.common import constants

METASTORE_NAME = "heron.package.metastore.name"
METASTORE_ROOT_PATH = "heron.package.metastore.root_path"

def get_metastore(conf):
  metastore = getattr(sys.modules[__name__], conf[METASTORE_NAME])
  instance = metastore(conf)
  return instance

class Metastore(object):
  # get all package names
  def get_packages(self, role, extra_info):
    raise NotImplementedError('get_packages is not implemented')

  # get all available version numbers for a package
  def get_versions(self, role, pkg_name, extra_info):
    raise NotImplementedError('get_versions is not implemented')

  # add meta data for a package's version
  def add_pkg_meta(self, role, pkg_name, location, description, extra_info):
    raise NotImplementedError('add_pkg_meta in not implemented')

  # delete meta data for a package's version
  def delete_pkg_meta(self, role, pkg_name, version, extra_info):
    raise NotImplementedError('delete_pkg_meta is not implemented')

  # get meta data for a package's version
  def get_pkg_meta(self, role, pkg_name, version, extra_info):
    raise NotImplementedError('get_pkg_meta is not implemented')

  # get package stored location
  def get_pkg_location(self, role, pkg_name, version, extra_info):
    raise NotImplementedError('get_pkg_location is not implemented')

  # set the tag on a package, only "LIVE" and "LATEST" tags are available
  def set_tag(self, tag, role, pkg_name, version, extra_info):
    raise NotImplementedError('set_tag is not implemented')

  # clean the tag on a package
  def unset_tag(self, tag, role, pkg_name, extra_info):
     raise NotImplementedError('unset_tag is not implemented')

  # get a pkg's info
  def get_meta_by_tag(self, tag, role, pkg_name, extra_info):
    raise NotImplementedError('get_meat_by_tag is not implemented')

  # get pkg uri
  def get_pkg_uri(self, scheme, role, pkg_name, version, extra_info):
    raise NotImplementedError('get_pkg_uri is not implemented')

class LocalMetastore(Metastore):
  def __init__(self, conf):
    self.meta_file = conf[METASTORE_ROOT_PATH]

    # create the file if missing
    if not os.path.exists(self.meta_file):
      with open(self.meta_file, "w") as mfile:
        mfile.write("{}")

  def get_packages(self, role, extra_info):
    packages = []

    with open(self.meta_file, "r") as meta_file:
      meta_info = json.load(meta_file)
      if role in meta_info:
        pkg_map = meta_info[role]
        for key, value in pkg_map.items():
          packages.append(key)
      else:
        Log.error("role %s not found" % role)

    return packages

  def get_versions(self, role, pkg_name, extra_info):
    versions = []

    with open(self.meta_file, "r") as meta_file:
      meta_info = json.load(meta_file)
      if role in meta_info and pkg_name in meta_info[role]:
        version_map = meta_info[role][pkg_name]
        for key, value in version_map.items():
          if key != constants.LIVE and \
             key != constants.LATEST and \
             key != constants.COUNTER:
            version_msg = "Version: %s" % key
            if version_map[key][constants.DELETED]:
              version_msg = version_msg + " DELETED"

            versions.append(version_msg)
      else:
        Log.error("role/pkg(%s/%s) not found" % (role, pkg_name))

    versions.sort()
    return versions

  def add_pkg_meta(self, role, pkg_name, description, location, extra_info):
    with open(self.meta_file, "r") as meta_file:
      meta_info = json.load(meta_file)

    if role not in meta_info:
      meta_info[role] = {}

    if pkg_name not in meta_info[role]:
      pkg_entry = {}
      pkg_entry[constants.LIVE] = None
      pkg_entry[constants.LATEST] = 0   # points to the latest valid version
      pkg_entry[constants.COUNTER] = 0      # points to the ever-incr version
      meta_info[role][pkg_name] = pkg_entry

    meta_info[role][pkg_name][constants.COUNTER] += 1
    version = str(meta_info[role][pkg_name][constants.COUNTER])

    version_entry = {}
    version_entry[constants.STORE_PATH] = location
    version_entry[constants.DELETED] = False
    version_entry[constants.DESC] = description
    meta_info[role][pkg_name][version] = version_entry
    meta_info[role][pkg_name][constants.LATEST] = version

    with open(self.meta_file, "w") as meta_file:
      json.dump(meta_info, meta_file)

    return True, version

  def get_pkg_meta(self, role, pkg_name, version, extra_info):
    # version is specified with tag, get the actual version
    with open(self.meta_file, "r") as meta_file:
      meta_info = json.load(meta_file)
      pkg_info = meta_info[role][pkg_name]

    version = self._translate_version(version, pkg_info)
    if not self._is_valid_version(pkg_info, version):
      Log.error("the requested version %s is not correct. Bailing out..." % version)
      sys.exit(1)

    return pkg_info[str(version)]

  def get_pkg_location(self, role, pkg_name, version, extra_info):
    with open(self.meta_file, "r") as meta_file:
      meta_info = json.load(meta_file)

    # verify pkg
    if role not in meta_info or pkg_name not in meta_info[role]:
      Log.error("requested package does not exist")
      return None

    # translate version
    pkg_info = meta_info[role][pkg_name]
    version = self._translate_version(version, pkg_info)

    if not self._is_valid_version(pkg_info, version):
      Log.error("requested version %s is not correct. Bailing out..." % version)
      return None

    # fetch location
    return pkg_info[str(version)][constants.STORE_PATH]

  def get_pkg_uri(self, scheme, role, pkg_name, version, extra_info):
    return "%s://%s/%s/%s" % (scheme, role, pkg_name, version)

  # delete is nothing but set the Deleted tag
  def delete_pkg_meta(self, role, pkg_name, version, extra_info):
    with open(self.meta_file, "r") as meta_file:
      meta_info = json.load(meta_file)
      pkg_info = meta_info[role][pkg_name]

    if not self._is_valid_version(pkg_info, version):
      Log.error("version doesn't exist")
      return False

    # delete live is not allowed
    if version == constants.LIVE or pkg_info[constants.LIVE] == version:
      Log.error("Cannot delete a 'live' version")
      return False

    # delete latest requires update of the latest tag
    if version == constants.LATEST or pkg_info[constants.LATEST] == version:
      version = pkg_info[constants.LATEST]
      pkg_info[constants.LATEST] = self._find_second_latest(pkg_info)

    if pkg_info[version][constants.DELETED]:
      Log.error("version already deleted")
      return False

    # mark version deletion
    meta_info[role][pkg_name][version][constants.DELETED] = True

    with open(self.meta_file, "w") as meta_file:
      json.dump(meta_info, meta_file)

    return True

  def _find_second_latest(self, pkg_info):
    if pkg_info[constants.LATEST] is None:
      return None

    latest = int(pkg_info[constants.LATEST])
    for v in range(latest - 1, 0, -1):
      version = str(v)
      if not pkg_info[version][constants.DELETED]:
        return version

    return None

  def get_meta_by_tag(self, tag, role, pkg_name, extra_info):
    with open(self.meta_file, "r") as meta_file:
      meta_info = json.load(meta_file)

    if not self._is_valid_pkg(meta_info, role, pkg_name, extra_info):
      Log.error('package info is not valid')
      return dict()

    pkg_info = meta_info[role][pkg_name]
    version = pkg_info[tag]
    if version is None:
      Log.error("The tag '%s' is unset. No package version found" % tag)
      return dict()
    else:
      info = pkg_info[version]
      info[constants.VERSION] = version
      return info

  def set_tag(self, tag, role, pkg_name, version, extra_info):
    return self._update_tag(tag, role, pkg_name, extra_info, version)

  def unset_tag(self, tag, role, pkg_name, extra_info):
    return self._update_tag(tag, role, pkg_name, extra_info, None, True)

  def _update_tag(self, tag, role, pkg_name, extra_info, version = None, is_reset = False):
    with open(self.meta_file, "r") as meta_file:
      meta_info = json.load(meta_file)

    if not self._is_valid_pkg(meta_info, role, pkg_name, extra_info):
      Log.error('package info is not valid')
      return False

    pkg_info = meta_info[role][pkg_name]

    if is_reset:
      meta_info[role][pkg_name][tag] = None
    else:
      if not self._is_valid_version(pkg_info, version):
        Log.error('version info is not valid')
        return False
      meta_info[role][pkg_name][tag] = version

    with open(self.meta_file, "w") as meta_file:
      json.dump(meta_info, meta_file)

    return True

  def _is_valid_pkg(self, meta_info, role, pkg_name, extra_info):
    valid_pkg = False
    if pkg_name in meta_info.get(role, {}):
      valid_pkg = True

    return valid_pkg

  def _is_valid_version(self, pkg_info, version):
    return version == constants.LATEST or \
           version == constants.LIVE or \
           pkg_info[constants.LATEST] >= version

  def _translate_version(self, version, pkg_info):
    if version == constants.LATEST:
      return pkg_info[constants.LATEST]
    elif version == constants.LIVE:
      if pkg_info[constants.LIVE] is None:
        Log.error("there's no live version for the pkg. Bailing out...")
        sys.exit(1)

      return pkg_info[constants.LIVE]
    else:
      return version