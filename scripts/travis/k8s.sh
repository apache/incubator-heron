#!/usr/bin/env bash
:<<'DOC'
set NO_CACHE=1 to always rebuild images.

DOC
set -o errexit -o nounset -o pipefail
TAG=test
HERE="$(cd "$(dirname "$0")"; pwd -P)"
ROOT="$(cd "$HERE/../.."; pwd -P)"

function bazel_file {
    # bazel_file VAR_NAME //some/build:target
    # this will set VAR_NAME to the path of the build artefact
    local var="${1:?}"
    local ref="${2:?}"
    local path="$(bazel info bazel-genfiles)/$(echo "${ref##//}" | tr ':' '/')"
    bazel build "$ref"
    eval "$var=$path"
}

function kind_images {
    # list all images in the kind registry
    docker exec -it kind-control-plane crictl images
}

function install_helm3 {
    pushd /tmp
        curl --location https://get.helm.sh/helm-v3.2.1-linux-amd64.tar.gz --output helm.tar.gz
        tar --extract --file=helm.tar.gz --strip-components=1 linux-amd64/helm
        mv helm ~/.local/bin/
    popd
}

function action {
    (
        tput setaf 4;
        echo "[$(date --rfc-3339=seconds)] $*";
        tput sgr0
    ) > /dev/stderr
}


function create_cluster {
    # trap "kind delete cluster" EXIT
    if [ -z "$(kind get clusters)" ]; then
        action "Creating kind cluster"
        kind create cluster --config="$0.kind.yaml"
    fi
}

function get_image {
    # cannot use `bazel_file heron_archive //scripts/images:heron.tar` as not distro image
    local tag="$TAG"
    local distro="${1:?}"
    local out
    local expected="$ROOT/dist/heron-docker-$tag-$distro.tar"
    if [ -f "$expected" ] && [ -z "${NO_CACHE-}" ]; then
        action "Using pre-existing heron image"
        out="$expected"
    else
        action "Creating heron image"
        local gz="$(scripts/release/docker-images build test debian10)"
        # XXX: must un .gz https://github.com/kubernetes-sigs/kind/issues/1636
        gzip --decompress "$gz"
        out="${gz%%.gz}"
    fi
    archive="$out"
}

create_cluster

get_image debian10
heron_archive="$archive"
action "Loading heron docker image"
kind load image-archive "$heron_archive"
#image_heron="docker.io/bazel/scripts/images:heron"
#image_heron="$heron_image"
image_heron="heron/heron:$TAG"

action "Loading bookkeeper image"
image_bookkeeper="docker.io/apache/bookkeeper:4.11.0"
docker pull "$image_bookkeeper"
kind load docker-image "$image_bookkeeper"

action "Deploying heron with helm"
# install heron in kind using helm
bazel_file helm_yaml //scripts/packages:index.yaml
helm install heron "$(dirname "$helm_yaml")/heron-0.0.0.tgz" \
    --set image="$image_heron" \
    --set imagePullPolicy=IfNotPresent \
    --set bookieReplicas=1 \
    --set zkReplicas=1
