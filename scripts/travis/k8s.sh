#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
:<<'DOC'
set NO_CACHE=1 to always rebuild images.
set DEBUG=1 to not clean up

This requires kubectl, and kind.

DOC
set -o errexit -o nounset -o pipefail
TAG=test
HERE="$(cd "$(dirname "$0")"; pwd -P)"
ROOT="$(git rev-parse --show-toplevel)"

CLUSTER_NAME="kubernetes"

API_URL="http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy"
TRACKER_URL="http://localhost:8001/api/v1/namespaces/default/services/heron-tracker:8888/proxy"
UI_URL="http://localhost:8001/api/v1/namespaces/default/services/heron-ui:8889/proxy"

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

function action {
    (
        tput setaf 4;
        echo "[$(date --rfc-3339=seconds)] $*";
        tput sgr0
    ) > /dev/stderr
}

function clean {
    if [ -z "${DEBUG-}" ]; then
        action "Cleaning up"
        kind delete cluster
        kill -TERM -$$
    else
        action "Not cleaning up due to DEBUG flag being set"
    fi
}

function create_cluster {
    # trap "kind delete cluster" EXIT
    if [ -z "$(kind get clusters)" ]; then
        action "Creating kind cluster"
        kind create cluster --config="$0.kind.yaml"
    else
        action "Using existing kind cluster"
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
        local gz="$(scripts/release/docker-images build test debian11)"
        # XXX: must un .gz https://github.com/kubernetes-sigs/kind/issues/1636
        gzip --decompress "$gz"
        out="${gz%%.gz}"
    fi
    archive="$out"
}

function url_wait {
    local seconds="$1"
    local url="$2"
    local retries=0
    tput sc
    while ! curl "$url" --location --fail --silent --output /dev/null --write-out "attempt $retries: %{http_code}"
    do
        retries=$((retries + 1))
        if [ $retries -eq 60 ]; then
            echo
            return 1
        fi
        sleep 1
        tput el1
        tput rc
        tput sc
    done
    echo
}

trap clean EXIT
create_cluster

get_image debian11
heron_archive="$archive"
action "Loading heron docker image"
kind load image-archive "$heron_archive"
#image_heron="docker.io/bazel/scripts/images:heron"
#image_heron="$heron_image"
image_heron="apache/heron:$TAG"

action "Loading bookkeeper image"
image_bookkeeper="docker.io/apache/bookkeeper:4.14.5"
docker pull "$image_bookkeeper"
kind load docker-image "$image_bookkeeper"

action "Deploying to kubernetes"
#kubectl create namespace default
kubectl config set-context kind-kind --namespace=default

# deploy
DIR=./deploy/kubernetes/minikube
sed "s#heron/heron:latest#$image_heron#g" ${DIR}/zookeeper.yaml > /tmp/zookeeper.yaml
sed "s#heron/heron:latest#$image_heron#g" ${DIR}/tools.yaml > /tmp/tools.yaml
sed "s#heron/heron:latest#$image_heron#g" ${DIR}/apiserver.yaml > /tmp/apiserver.yaml

kubectl proxy -p 8001 &

kubectl create -f /tmp/zookeeper.yaml
kubectl get pods
kubectl create -f ${DIR}/bookkeeper.yaml
kubectl create -f /tmp/tools.yaml
kubectl create -f /tmp/apiserver.yaml

action "waiting for API server"
url_wait 120 "$API_URL/api/v1/version"
action "waiting for UI"
url_wait 120 "$UI_URL/proxy"

kubectl port-forward service/zookeeper 2181:2181 &

action "API:       $API_URL"
action "Tracker:   $TRACKER_URL"
action "UI:        $UI_URL"
action "Zookeeper: 127.0.0.1:2181"

heron config "$CLUSTER_NAME" set service_url "$API_URL"

"$HERE/test.sh"
