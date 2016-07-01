# Copyright 2015 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set help URL
getting_started_url=http://heronstreaming.io/docs/getting-started

# Heron self-extractable installer for api package
function usage() {
  echo "Usage: $progname [options]" >&2
  echo "Options are:" >&2
  echo "  --prefix=/some/path set the prefix path (default=/usr/local)." >&2
  echo "  --user configure for user install, expands to" >&2
  echo '           `--prefix=$HOME/.heronapi`.' >&2
  echo "  --maven install jars to maven local repo" >&2
  exit 1
}

# Test for maven dependencies
function check_maven() {
  if ! which mvn >/dev/null; then
     echo >&2
     echo "maven not found, please install the corresponding package." >&2
    echo "See $getting_started_url for more information on" >&2
    echo "dependencies of Heron." >&2
    exit 1
  fi
}

function install_to_local() {
  # Test for write access
  test_write "${base}"

  # Do the actual installation
  echo -n "Uncompressing."

  # Cleaning-up, with some guards.
  if [ -d "${base}" -a -x "${base}/lib/heron-api.jar" ]; then
    rm -fr "${base}"
  fi

  mkdir -p ${base}
  echo -n .

  unzip -q -o "${BASH_SOURCE[0]}" -d "${base}"
  untar ${base}/heron-api.tar.gz ${base}
  echo -n .
  chmod -R og-w "${base}"
  chmod -R og+rX "${base}"
  chmod -R u+rwX "${base}"
  echo -n .

  rm "${base}/heron-api.tar.gz"
}

function install_to_maven() {
  echo "Installing jars to local maven repo." >&2

  # Uncompress from zip
  tmp_dir=`mktemp -d -t heron.XXXX`
  unzip -q -o "${BASH_SOURCE[0]}" -d "${tmp_dir}"
  untar ${tmp_dir}/heron-api.tar.gz ${tmp_dir}

  # Install into maven local
  mvn install:install-file -q -Dfile="${tmp_dir}/heron-api.jar" -DgroupId="com.twitter.heron" \
    -DartifactId="heron-api" -Dversion="SNAPSHOT" -Dpackaging="jar"

  mvn install:install-file -q -Dfile="${tmp_dir}/heron-spi.jar" -DgroupId="com.twitter.heron" \
    -DartifactId="heron-spi" -Dversion="SNAPSHOT" -Dpackaging="jar"

  mvn install:install-file -q -Dfile="${tmp_dir}/heron-storm.jar" -DgroupId="com.twitter.heron" \
    -DartifactId="heron-storm" -Dversion="SNAPSHOT" -Dpackaging="jar"

  # clean tmp files
  rm -rf "${tmp_dir}"
}

# Installation and etc prefix can be overriden from command line
install_prefix=${1:-"/usr/local/heronapi"}

progname="$0"

echo "Heron API installer"
echo "---------------------"
echo

prefix="/usr/local"
base="%prefix%/heronapi"
use_maven=false

for opt in "${@}"; do
  case $opt in
    --prefix=*)
      prefix="$(echo "$opt" | cut -d '=' -f 2-)"
      ;;
    --user)
      base="$HOME/.heronapi"
      ;;
    --maven)
      use_maven=true
      ;;
    *)
      usage
      ;;
  esac
done

base="${base//%prefix%/${prefix}}"

check_unzip; check_tar; check_java

if [ "$use_maven" == true ]; then
  check_maven
  install_to_maven
else
  install_to_local
fi

cat <<EOF

Heron API is now installed!

See ${getting_started_url} for how to use Heron.
EOF
echo
cat <<'EOF'
%release_info%
EOF
exit 0
