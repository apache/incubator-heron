#!/bin/bash -e

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

# Heron self-extractable installer

# Installation and etc prefix can be overriden from command line
install_prefix=${1:-"/usr/local/heron"}
heronrc=${2:-"/usr/local/heron/etc/heron.heronrc"}

progname="$0"

echo "Heron installer"
echo "---------------"
echo
cat <<'EOF'
%release_info%
EOF

function usage() {
  echo "Usage: $progname [options]" >&2
  echo "Options are:" >&2
  echo "  --prefix=/some/path set the prefix path (default=/usr/local/heron)." >&2
  echo "  --heronrc= set the heronrc path (default=/usr/local/heron/etc/heron.heronrc)." >&2
  echo "  --user configure for user install, expands to" >&2
  echo '           `--prefix=$HOME/.heron --heronrc=$HOME/.heronrc`.' >&2
  exit 1
}

prefix="/usr/local/heron"
base="%prefix%"
bin="%prefix%/bin"
conf="%prefix%/conf"
heronrc="/usr/local/heron/etc/heron.heronrc"

for opt in "${@}"; do
  case $opt in
    --prefix=*)
      prefix="$(echo "$opt" | cut -d '=' -f 2-)"
      ;;
    --heronrc=*)
      heronrc="$(echo "$opt" | cut -d '=' -f 2-)"
      ;;
    --user)
      bin="$HOME/.heron/bin"
      base="$HOME/.heron"
      heronrc="$HOME/.heronrc"
      ;;
    *)
      usage
      ;;
  esac
done

base="${base//%prefix%/${prefix}}"
heronrc="${heronrc//%prefix%/${prefix}}"

function test_write() {
  local file="$1"
  while [ "$file" != "/" ] && [ -n "${file}" ] && [ ! -e "$file" ]; do
    file="$(dirname "${file}")"
  done
  [ -w "${file}" ] || {
    echo >&2
    echo "The Heron installer must have write access to $1!" >&2
    echo >&2
    usage
  }
}

# Test for dependencies
# unzip
if ! which unzip >/dev/null; then
  echo >&2
  echo "unzip not found, please install the corresponding package." >&2
  echo "See http://heron.github.io/docs/install.html for more information on" >&2
  echo "dependencies of Heron." >&2
  exit 1
fi

# java
if [ -z "${JAVA_HOME-}" ]; then
  case "$(uname -s | tr 'A-Z' 'a-z')" in
    linux)
      JAVA_HOME="$(readlink -f $(which javac) 2>/dev/null | sed 's_/bin/javac__')" || true
      BASHRC="~/.bashrc"
      ;;
    freebsd)
      JAVA_HOME="/usr/local/openjdk8"
      BASHRC="~/.bashrc"
      ;;
    darwin)
      JAVA_HOME="$(/usr/libexec/java_home -v ${JAVA_VERSION}+ 2> /dev/null)" || true
      BASHRC="~/.bash_profile"
      ;;
  esac
fi
if [ ! -x "${JAVA_HOME}/bin/javac" ]; then
  echo >&2
  echo "Java not found, please install the corresponding package" >&2
  echo "See http://heron.github.io/docs/install.html for more information on" >&2
  echo "dependencies of Heron." >&2
  exit 1
fi

# Test for write access
test_write "${base}"
test_write "${heronrc}"

# Do the actual installation
echo -n "Uncompressing."

# Cleaning-up, with some guards.
if [ -d "${base}" -a -x "${base}/bin/heron-cli2" ]; then
  rm -fr "${base}"
fi

mkdir -p ${base} ${base}/bin ${base}/etc ${base}/lib ${base}/conf
echo -n .

unzip -q -o "${BASH_SOURCE[0]}" -d "${base}"
echo -n .
cat >"${base}/etc/heron.heronrc" <<EO
build --package_path %workspace%:${base}/base_workspace
fetch --package_path %workspace%:${base}/base_workspace
query --package_path %workspace%:${base}/base_workspace
EO
echo -n .
chmod -R og-w "${base}"
chmod -R og+rX "${base}"
chmod -R u+rwX "${base}"
echo -n .

#ln -s "${base}/bin/bazel" "${bin}/bazel"
echo -n .

# Uncompress the bazel base install for faster startup time
#"${bin}/bazel" help >/dev/null

if [ -f "${heronrc}" ]; then
  echo
  echo "${heronrc} already exists, ignoring. It is either a link to"
  echo "${base}/etc/bazel.heronrc or that it's importing that file with:"
  echo "  import ${base}/etc/bazel.heronrc"
else
  ln -s "${base}/etc/heron.heronrc" "${heronrc}"
  echo .
fi

cat <<EOF

Heron is now installed!

Make sure you have "${bin}" in your path. 

See http://heron.github.io/docs/getting-started.html to start a new project!
EOF
exit 0
