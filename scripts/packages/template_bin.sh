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

# Heron self-extractable installer for client package

# Set help URL
getting_started_url=https://heron.apache.org/docs/getting-started-local-single-node/

# Installation and etc prefix can be overriden from command line
install_prefix=${1:-"/usr/local/heron"}

progname="$0"

echo "Heron installer"
echo "---------------"
echo

function usage() {
  echo "Usage: $progname [options]" >&2
  echo "Options are:" >&2
  echo "  --prefix=/some/path set the prefix path (default=/usr/local)." >&2
  echo "  --user configure for user install, expands to" >&2
  echo '           `--prefix=$HOME/.heron`.' >&2
  exit 1
}

prefix="/usr/local"
bin="%prefix%/bin"
base="%prefix%/heron"
conf="%prefix%/heron/conf"

for opt in "${@}"; do
  case $opt in
    --prefix=*)
      prefix="$(echo "$opt" | cut -d '=' -f 2-)"
      ;;
    --user)
      bin="$HOME/bin"
      base="$HOME/.heron"
      ;;
    *)
      usage
      ;;
  esac
done

bin="${bin//%prefix%/${prefix}}"
base="${base//%prefix%/${prefix}}"

check_unzip; check_tar; check_java

# Test for write access
test_write "${bin}"
test_write "${base}"

# Do the actual installation
echo -n "Cleaning up symlinks in ${bin}, if present."

# Cleaning-up, with some guards.
if [ -L "${bin}/heron" ]; then
  rm -f "${bin}/heron"
fi

if [ -L "${bin}/heron-explorer" ]; then
  rm -f "${bin}/heron-explorer"
fi

if [ -L "${bin}/heron-tracker" ]; then
  rm -f "${bin}/heron-tracker"
fi

if [ -L "${bin}/heron-ui" ]; then
  rm -f "${bin}/heron-ui"
fi

if [ -L "${bin}/heron-apiserver" ]; then
  rm -f "${bin}/heron-apiserver"
fi

if [ -d "${base}" -a -x "${base}/bin/heron" ]; then
  rm -fr "${base}"
fi

mkdir -p ${bin} ${base} ${base}/etc
echo "..done"

echo -n "Uncompressing heron package."
unzip -q -o "${BASH_SOURCE[0]}" -d "${base}"
untar ${base}/heron.tar.gz ${base}
echo "..done"
chmod 0755 ${base}/bin/heron
chmod 0755 ${base}/bin/heron-explorer
chmod 0755 ${base}/bin/heron-tracker
chmod 0755 ${base}/bin/heron-ui
chmod 0755 ${base}/bin/heron-apiserver
chmod -R og-w "${base}"
chmod -R og+rX "${base}"
chmod -R u+rwX "${base}"

ln -s "${base}/bin/heron" "${bin}/heron"
ln -s "${base}/bin/heron-explorer" "${bin}/heron-explorer"
ln -s "${base}/bin/heron-tracker" "${bin}/heron-tracker"
ln -s "${base}/bin/heron-ui" "${bin}/heron-ui"
ln -s "${base}/bin/heron-apiserver" "${bin}/heron-apiserver"

echo -n "Uncompressing heron core."
untar ${base}/dist/heron-core.tar.gz ${base}/dist
echo "..done"

rm "${base}/heron.tar.gz"
rm -f "${base}/dist/release.yaml"

cat <<EOF

Heron is now installed!

Make sure you have "${bin}" in your path.

See ${getting_started_url} for how to use Heron.
EOF
echo
cat <<'EOF'
%release_info%
EOF
exit 0
